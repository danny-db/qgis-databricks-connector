"""
Databricks Genie Chat — natural-language interface for querying data via the
Databricks Genie API, with optional geometry visualisation as QGIS layers.

All Genie-related code lives in this single module, consistent with the
existing plugin structure (one feature per file).
"""
import json
import re
import time
import urllib.request
import urllib.error
import urllib.parse
import decimal as _decimal
import datetime as _datetime
import json as _json

from qgis.PyQt.QtCore import (
    Qt, QThread, pyqtSignal, QSettings, QVariant, QDateTime, QDate, QTimer
)
from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QComboBox, QTableWidget, QTableWidgetItem,
    QMessageBox, QHeaderView, QTextBrowser, QApplication,
    QSizePolicy, QAbstractItemView
)
from qgis.core import (
    QgsVectorLayer, QgsProject, QgsMessageLog, Qgis,
    QgsFeature, QgsFields, QgsField, QgsGeometry,
    QgsCoordinateReferenceSystem
)


# ---------------------------------------------------------------------------
# Helpers (reused from databricks_dialog.py patterns)
# ---------------------------------------------------------------------------

def _coerce_attr(value):
    """Convert Python values to QGIS-compatible types (Qt5 & Qt6)."""
    if value is None:
        return None
    if isinstance(value, _decimal.Decimal):
        return float(value)
    if isinstance(value, _datetime.datetime):
        return QDateTime(value.year, value.month, value.day,
                         value.hour, value.minute, value.second)
    if isinstance(value, _datetime.date):
        return QDate(value.year, value.month, value.day)
    if isinstance(value, _datetime.timedelta):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (list, dict)):
        return _json.dumps(value)
    return value


def _is_wkt_format(value_str):
    """Return True if *value_str* looks like WKT (with optional SRID prefix)."""
    if not isinstance(value_str, str):
        return False
    s = value_str.strip().upper()
    if s.startswith('SRID='):
        parts = s.split(';', 1)
        if len(parts) > 1:
            s = parts[1].strip()
    prefixes = (
        'POINT', 'LINESTRING', 'POLYGON',
        'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON',
        'GEOMETRYCOLLECTION',
    )
    return any(s.startswith(p) for p in prefixes)


def _strip_srid_from_wkt(wkt_str):
    """Strip ``SRID=…;`` prefix from a WKT string."""
    if not isinstance(wkt_str, str):
        return wkt_str
    wkt_str = wkt_str.strip()
    if wkt_str.upper().startswith('SRID='):
        parts = wkt_str.split(';', 1)
        if len(parts) > 1:
            return parts[1].strip()
    return wkt_str


# Column names that commonly hold geometry data.
_GEOM_COL_NAMES = frozenset([
    'geometry', 'geom', 'wkt', 'geography', 'shape',
    'location', 'point', 'polygon',
])


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _api_request(hostname, path, access_token, method='GET', body=None,
                 timeout=30):
    """Execute a Databricks REST API call and return the parsed JSON body.

    Raises ``urllib.error.URLError`` or ``RuntimeError`` on failure.
    """
    url = f"https://{hostname}{path}"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }
    data = json.dumps(body).encode('utf-8') if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode('utf-8')
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        # Read the error body for better messages
        err_body = ''
        try:
            err_body = exc.read().decode('utf-8', errors='replace')
        except Exception:
            pass
        if exc.code in (401, 403):
            raise RuntimeError(
                f"Authentication failed ({exc.code}). "
                "Check your access token and permissions."
            )
        if exc.code == 429:
            raise RuntimeError(
                "Rate limited (429). Please wait a moment before trying again."
            )
        raise RuntimeError(
            f"HTTP {exc.code}: {exc.reason}\n{err_body}"
        )


# ---------------------------------------------------------------------------
# QThread: fetch available Genie Spaces
# ---------------------------------------------------------------------------

class GenieSpaceListThread(QThread):
    """Fetch the list of Genie Spaces the user can access."""

    spaces_loaded = pyqtSignal(list)   # [{id, title}, ...]
    error_occurred = pyqtSignal(str)

    def __init__(self, hostname, access_token, parent=None):
        super().__init__(parent)
        self.hostname = hostname
        self.access_token = access_token

    def run(self):
        try:
            data = _api_request(
                self.hostname,
                '/api/2.0/genie/spaces',
                self.access_token,
            )
            spaces = []
            for sp in data.get('spaces', []):
                spaces.append({
                    'id': sp.get('space_id', sp.get('id', '')),
                    'title': sp.get('title', sp.get('name', 'Untitled')),
                })
            self.spaces_loaded.emit(spaces)
        except Exception as exc:
            self.error_occurred.emit(str(exc))


# ---------------------------------------------------------------------------
# QThread: Genie conversation API (start / send / poll / fetch results)
# ---------------------------------------------------------------------------

class GenieApiThread(QThread):
    """Drive a single Genie question through the REST API and return results."""

    response_received = pyqtSignal(dict)   # parsed response dict
    error_occurred = pyqtSignal(str)
    status_update = pyqtSignal(str)        # transient status text

    # Max polling duration (seconds)
    _MAX_POLL_SECS = 600  # 10 min

    def __init__(self, hostname, access_token, space_id, question,
                 conversation_id=None, parent=None):
        super().__init__(parent)
        self.hostname = hostname
        self.access_token = access_token
        self.space_id = space_id
        self.question = question
        self.conversation_id = conversation_id
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    # -- internal helpers ---------------------------------------------------

    def _api(self, path, method='GET', body=None, timeout=30):
        return _api_request(self.hostname, path, self.access_token,
                            method=method, body=body, timeout=timeout)

    def _poll_message(self, conversation_id, message_id):
        """Poll until the message reaches a terminal state.  Returns the
        full message payload."""
        base = (f'/api/2.0/genie/spaces/{self.space_id}'
                f'/conversations/{conversation_id}/messages/{message_id}')
        delay = 1.0
        elapsed = 0.0
        while elapsed < self._MAX_POLL_SECS:
            if self._cancelled:
                raise RuntimeError("Cancelled by user.")
            self.status_update.emit("Waiting for Genie response")
            time.sleep(delay)
            elapsed += delay
            msg = self._api(base)
            status = msg.get('status', '').upper()
            if status in ('COMPLETED', 'COMPLETE'):
                return msg
            if status in ('FAILED', 'CANCELLED', 'CANCELED'):
                err = msg.get('error', {}).get('message', status)
                raise RuntimeError(f"Genie returned status {status}: {err}")
            # Exponential backoff capped at 5 s
            delay = min(delay * 1.5, 5.0)
        raise RuntimeError("Genie response timed out after 10 minutes.")

    def _fetch_query_result(self, conversation_id, message_id, attachment_id):
        """GET the query-result attachment and return (columns, rows)."""
        path = (f'/api/2.0/genie/spaces/{self.space_id}'
                f'/conversations/{conversation_id}'
                f'/messages/{message_id}'
                f'/query-result/{attachment_id}')
        data = self._api(path, timeout=60)
        # The response has a "statement_response" wrapper with manifest & result
        stmt = data.get('statement_response', data)
        manifest = stmt.get('manifest', {})
        columns = [c.get('name', f'col_{i}')
                    for i, c in enumerate(manifest.get('schema', {}).get('columns', []))]
        raw_chunks = stmt.get('result', {}).get('data_array', [])
        rows = raw_chunks  # list of lists
        return columns, rows

    # -- main entry ---------------------------------------------------------

    def run(self):
        try:
            # 1. Start or continue conversation
            if self.conversation_id:
                path = (f'/api/2.0/genie/spaces/{self.space_id}'
                        f'/conversations/{self.conversation_id}/messages')
            else:
                path = (f'/api/2.0/genie/spaces/{self.space_id}'
                        f'/start-conversation')

            self.status_update.emit("Sending question to Genie")
            resp = self._api(path, method='POST',
                             body={'content': self.question}, timeout=60)

            conversation_id = resp.get('conversation_id',
                                       self.conversation_id or '')
            message_id = resp.get('message_id', resp.get('id', ''))

            # 2. Poll for completion
            msg = self._poll_message(conversation_id, message_id)

            # 3. Extract content text and SQL
            content_text = ''
            query_statement = ''
            attachments = msg.get('attachments', [])

            for att in attachments:
                if att.get('text', {}).get('content'):
                    content_text = att['text']['content']
                if att.get('query', {}).get('query'):
                    query_statement = att['query']['query']

            # Fallback: some API versions put content at top level
            if not content_text:
                content_text = msg.get('content', '')

            # 4. Fetch query results if available
            columns = []
            rows = []
            attachment_id = None
            for att in attachments:
                aid = att.get('attachment_id', att.get('id'))
                if att.get('query') and aid:
                    attachment_id = aid
                    break

            if attachment_id:
                self.status_update.emit("Fetching query results")
                columns, rows = self._fetch_query_result(
                    conversation_id, message_id, attachment_id)

            self.response_received.emit({
                'conversation_id': conversation_id,
                'message_id': message_id,
                'content': content_text,
                'query_statement': query_statement,
                'columns': columns,
                'rows': rows,
            })

        except Exception as exc:
            self.error_occurred.emit(str(exc))


# ---------------------------------------------------------------------------
# QThread: re-query with ST_ASWKT (Path B only — network I/O)
# ---------------------------------------------------------------------------

class GenieReQueryThread(QThread):
    """Re-execute a query wrapping the geometry column with ST_ASWKT().

    Only performs the database query on the background thread.  The actual
    QgsVectorLayer creation must happen on the main thread (via the signal).
    """

    data_ready = pyqtSignal(list, list)   # (columns, rows) after re-query
    error_occurred = pyqtSignal(str)
    status_update = pyqtSignal(str)

    def __init__(self, hostname, http_path, access_token,
                 query_statement, geom_col, parent=None):
        super().__init__(parent)
        self.hostname = hostname
        self.http_path = http_path
        self.access_token = access_token
        self.query_statement = query_statement
        self.geom_col = geom_col

    def run(self):
        try:
            self.status_update.emit("Re-querying with ST_ASWKT wrapping...")

            try:
                from databricks import sql as dbsql
            except ImportError:
                self.error_occurred.emit(
                    "databricks-sql-connector is required for non-WKT geometry "
                    "re-query but is not installed.")
                return

            escaped_geom = f"`{self.geom_col.strip('`')}`"
            wrapped_query = (
                f"SELECT *, ST_ASWKT({escaped_geom}) AS __genie_wkt "
                f"FROM ({self.query_statement}) __genie_sub"
            )

            conn = dbsql.connect(
                server_hostname=self.hostname,
                http_path=self.http_path,
                access_token=self.access_token,
            )
            with conn.cursor() as cursor:
                cursor.execute(wrapped_query)
                columns = [d[0] for d in cursor.description]
                rows = [list(r) for r in cursor.fetchall()]
            conn.close()

            self.data_ready.emit(columns, rows)

        except Exception as exc:
            self.error_occurred.emit(f"Re-query failed: {exc}")


# ---------------------------------------------------------------------------
# GenieDialog — non-modal chat UI
# ---------------------------------------------------------------------------

class GenieDialog(QDialog):
    """Non-modal dialog providing a natural-language chat interface to
    Databricks Genie, with results preview and layer creation."""

    def __init__(self, iface, parent=None):
        super().__init__(parent)
        self.iface = iface
        self.settings = QSettings()

        # State
        self._conversation_id = None
        self._current_columns = []
        self._current_rows = []
        self._current_query = ''
        self._api_thread = None
        self._space_thread = None
        self._requery_thread = None

        # Chat history — list of HTML fragments, re-rendered via setHtml()
        self._chat_parts = []

        # Thinking animation state
        self._thinking_active = False
        self._thinking_start = 0.0
        self._thinking_dots = 0
        self._thinking_phase_text = ''
        self._thinking_timer = QTimer(self)
        self._thinking_timer.timeout.connect(self._on_thinking_tick)

        self._setup_ui()
        self._load_connections()

    # -- UI Setup -----------------------------------------------------------

    def _setup_ui(self):
        self.setWindowTitle("Databricks Genie Chat")
        self.setMinimumSize(720, 560)
        self.resize(800, 620)

        root = QVBoxLayout(self)

        # ── Row 1: Connection + Genie Space ───────────────────────────────
        conn_row = QHBoxLayout()
        conn_row.addWidget(QLabel("Connection:"))
        self.conn_combo = QComboBox()
        self.conn_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.conn_combo.currentIndexChanged.connect(self._on_connection_changed)
        conn_row.addWidget(self.conn_combo)

        conn_row.addWidget(QLabel("Genie Space:"))
        self.space_combo = QComboBox()
        self.space_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        conn_row.addWidget(self.space_combo)
        root.addLayout(conn_row)

        # ── Chat history ──────────────────────────────────────────────────
        self.chat_browser = QTextBrowser()
        self.chat_browser.setOpenExternalLinks(False)
        self.chat_browser.setReadOnly(True)
        self.chat_browser.setMinimumHeight(180)
        root.addWidget(self.chat_browser, stretch=3)

        # ── SQL panel (collapsed by default) ──────────────────────────────
        self.sql_toggle_btn = QPushButton("Show SQL")
        self.sql_toggle_btn.setEnabled(False)
        self.sql_toggle_btn.setFixedWidth(90)
        self.sql_toggle_btn.clicked.connect(self._on_toggle_sql)

        self.sql_browser = QTextBrowser()
        self.sql_browser.setReadOnly(True)
        self.sql_browser.setVisible(False)
        self.sql_browser.setMaximumHeight(120)

        self.copy_sql_btn = QPushButton("Copy SQL")
        self.copy_sql_btn.setAutoDefault(False)
        self.copy_sql_btn.setEnabled(False)
        self.copy_sql_btn.clicked.connect(self._on_copy_sql)

        sql_header = QHBoxLayout()
        sql_header.addWidget(self.sql_toggle_btn)
        sql_header.addWidget(self.copy_sql_btn)
        sql_header.addStretch()
        root.addLayout(sql_header)
        root.addWidget(self.sql_browser)

        # ── Results table ─────────────────────────────────────────────────
        self.results_table = QTableWidget()
        self.results_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.results_table.setMinimumHeight(100)
        self.results_table.horizontalHeader().setStretchLastSection(True)
        root.addWidget(self.results_table, stretch=2)

        # ── Row 3: Question input ─────────────────────────────────────────
        q_row = QHBoxLayout()
        q_row.addWidget(QLabel("Question:"))
        self.question_edit = QLineEdit()
        self.question_edit.setPlaceholderText(
            "Ask a question about your data...")
        self.question_edit.returnPressed.connect(self._on_ask_or_cancel)
        q_row.addWidget(self.question_edit)

        self.ask_btn = QPushButton("Ask")
        self.ask_btn.setAutoDefault(False)
        self.ask_btn.setDefault(False)
        self.ask_btn.clicked.connect(self._on_ask_or_cancel)
        q_row.addWidget(self.ask_btn)

        self.clear_btn = QPushButton("Clear Chat")
        self.clear_btn.setAutoDefault(False)
        self.clear_btn.setDefault(False)
        self.clear_btn.clicked.connect(self._on_clear_chat)
        q_row.addWidget(self.clear_btn)
        root.addLayout(q_row)

        # ── Row 4: Geometry + Layer + Copy SQL + Status ───────────────────
        bottom_row = QHBoxLayout()
        bottom_row.addWidget(QLabel("Geometry col:"))
        self.geom_combo = QComboBox()
        self.geom_combo.setMinimumWidth(140)
        bottom_row.addWidget(self.geom_combo)

        self.add_layer_btn = QPushButton("Add as Layer")
        self.add_layer_btn.setAutoDefault(False)
        self.add_layer_btn.setEnabled(False)
        self.add_layer_btn.clicked.connect(self._on_add_layer)
        bottom_row.addWidget(self.add_layer_btn)

        bottom_row.addStretch()
        self.status_label = QLabel("Status: Ready")
        bottom_row.addWidget(self.status_label)
        root.addLayout(bottom_row)

    # -- Connection helpers -------------------------------------------------

    def _load_connections(self):
        """Populate the connection dropdown from QSettings."""
        self.conn_combo.blockSignals(True)
        self.conn_combo.clear()
        self.settings.beginGroup("DatabricksConnector/Connections")
        names = self.settings.childGroups()
        self.settings.endGroup()
        for name in sorted(names):
            self.conn_combo.addItem(name)
        self.conn_combo.blockSignals(False)
        if self.conn_combo.count() > 0:
            self._on_connection_changed(0)

    def _get_connection(self):
        """Return (hostname, http_path, access_token) for the selected connection."""
        name = self.conn_combo.currentText()
        if not name:
            return None, None, None
        base = f"DatabricksConnector/Connections/{name}"
        hostname = self.settings.value(f"{base}/hostname", "")
        http_path = self.settings.value(f"{base}/http_path", "")
        token = self.settings.value(f"{base}/access_token", "")
        return hostname, http_path, token

    def _on_connection_changed(self, _index):
        """When connection changes, refresh the Genie Space list."""
        hostname, _hp, token = self._get_connection()
        if not hostname or not token:
            self.space_combo.clear()
            return
        self._fetch_spaces(hostname, token)

    def _fetch_spaces(self, hostname, token):
        """Kick off a background thread to list Genie Spaces."""
        self.space_combo.clear()
        self.space_combo.addItem("Loading...")
        self.space_combo.setEnabled(False)

        self._space_thread = GenieSpaceListThread(hostname, token, self)
        self._space_thread.spaces_loaded.connect(self._on_spaces_loaded)
        self._space_thread.error_occurred.connect(self._on_spaces_error)
        self._space_thread.start()

    def _on_spaces_loaded(self, spaces):
        self.space_combo.clear()
        self.space_combo.setEnabled(True)
        if not spaces:
            self.space_combo.addItem("(no spaces found)")
            return
        for sp in spaces:
            self.space_combo.addItem(sp['title'], sp['id'])

    def _on_spaces_error(self, msg):
        self.space_combo.clear()
        self.space_combo.setEnabled(True)
        self.space_combo.addItem("(error loading spaces)")
        self.status_label.setText(f"Status: {msg}")
        QgsMessageLog.logMessage(
            f"Genie space list error: {msg}",
            "Databricks Connector", Qgis.Warning)

    # -- Thinking indicator -------------------------------------------------

    def _start_thinking(self):
        """Show a thinking indicator in chat and animate the status bar."""
        self._thinking_active = True
        self._thinking_start = time.time()
        self._thinking_dots = 0
        self._thinking_phase_text = 'Genie is thinking'

        # Append a placeholder to the chat
        self._chat_parts.append(self._thinking_html())
        self._render_chat()

        # Start the animation timer (every 400 ms)
        self._thinking_timer.start(400)

    def _stop_thinking(self):
        """Remove the thinking indicator from chat."""
        self._thinking_timer.stop()
        self._thinking_active = False
        # Remove the thinking placeholder (always the last entry)
        if self._chat_parts and '<!-- genie-thinking -->' in self._chat_parts[-1]:
            self._chat_parts.pop()
        # Don't re-render yet — the caller will append the real response

    def _thinking_html(self):
        """Build the HTML for the current thinking state."""
        dots = '.' * ((self._thinking_dots % 3) + 1)
        elapsed = int(time.time() - self._thinking_start)
        phase = self._thinking_phase_text
        return (
            '<!-- genie-thinking -->'
            '<table width="100%" cellpadding="8" cellspacing="0">'
            '<tr><td bgcolor="#E8E8E8">'
            f'<i><font color="#555555">{phase}{dots}</font></i>'
            f' <font color="#777777">({elapsed}s)</font>'
            '</td></tr></table>'
        )

    def _on_thinking_tick(self):
        """Timer callback — update thinking indicator and status bar."""
        self._thinking_dots += 1
        dots = '.' * ((self._thinking_dots % 3) + 1)
        elapsed = int(time.time() - self._thinking_start)
        phase = self._thinking_phase_text

        # Update status bar
        self.status_label.setText(f"Status: {phase}{dots} ({elapsed}s)")

        # Update the thinking placeholder in chat (always last entry)
        if (self._chat_parts
                and '<!-- genie-thinking -->' in self._chat_parts[-1]):
            self._chat_parts[-1] = self._thinking_html()
            self._render_chat()

    # -- Ask / Cancel -------------------------------------------------------

    def _on_ask_or_cancel(self):
        """Handles both Ask and Cancel depending on current state."""
        if self._thinking_active:
            self._do_cancel()
        else:
            self._do_ask()

    def _do_cancel(self):
        """Cancel the in-flight Genie request."""
        if self._api_thread and self._api_thread.isRunning():
            self._api_thread.cancel()
        self._stop_thinking()
        self._chat_parts.append(
            '<p style="color:#999; font-style:italic;">Cancelled.</p>')
        self._render_chat()
        self._restore_ask_state()

    def _do_ask(self):
        question = self.question_edit.text().strip()
        if not question:
            return

        hostname, http_path, token = self._get_connection()
        if not hostname or not token:
            QMessageBox.warning(self, "No Connection",
                                "Please select a saved connection first.")
            return

        space_id = self.space_combo.currentData()
        if not space_id:
            QMessageBox.warning(self, "No Genie Space",
                                "Please select a Genie Space.")
            return

        # Disable controls, switch Ask → Cancel
        self.question_edit.setEnabled(False)
        self.add_layer_btn.setEnabled(False)
        self.copy_sql_btn.setEnabled(False)
        self.ask_btn.setText("Cancel")

        # Append user's original question to chat
        self._append_chat_user(question)
        self.question_edit.clear()

        # Append geometry hint so Genie returns geometry as a recognisable column
        api_question = (
            question
            + ' If there is a coordinates / geometry in the result,'
            ' make sure it returns as an additional column as the'
            ' geometry datatype.'
        )

        # Show thinking indicator
        self._start_thinking()

        # Fire API thread
        self._api_thread = GenieApiThread(
            hostname, token, space_id, api_question,
            conversation_id=self._conversation_id,
            parent=self,
        )
        self._api_thread.response_received.connect(self._on_response)
        self._api_thread.error_occurred.connect(self._on_api_error)
        self._api_thread.status_update.connect(self._on_api_status)
        self._api_thread.start()

    def _on_api_status(self, text):
        """Update the thinking phase text from the API thread."""
        self._thinking_phase_text = text

    def _restore_ask_state(self):
        """Re-enable controls after a request completes or is cancelled."""
        self.ask_btn.setText("Ask")
        self.ask_btn.setEnabled(True)
        self.question_edit.setEnabled(True)
        self.status_label.setText("Status: Ready")

    def _on_response(self, result):
        """Handle a completed Genie response."""
        self._stop_thinking()
        self._restore_ask_state()

        self._conversation_id = result.get('conversation_id')
        content = result.get('content', '')
        sql_text = result.get('query_statement', '')
        columns = result.get('columns', [])
        rows = result.get('rows', [])

        # Store for layer creation
        self._current_columns = columns
        self._current_rows = rows
        self._current_query = sql_text

        # Append Genie response to chat (text only, SQL in separate panel)
        self._append_chat_genie(content)

        # Update collapsible SQL panel
        self._update_sql_panel(sql_text)

        # Populate results table
        self._populate_results(columns, rows)

        # Auto-detect geometry column
        self._populate_geom_combo(columns, rows)

        # Enable Add as Layer if we have data
        self.add_layer_btn.setEnabled(len(rows) > 0)

    def _on_api_error(self, msg):
        self._stop_thinking()
        self._restore_ask_state()
        self.status_label.setText("Status: Error")
        self._append_chat_error(msg)
        QgsMessageLog.logMessage(
            f"Genie API error: {msg}",
            "Databricks Connector", Qgis.Warning)

    # -- Copy / Toggle SQL --------------------------------------------------

    def _on_copy_sql(self):
        if self._current_query:
            QApplication.clipboard().setText(self._current_query)
            self.status_label.setText("Status: SQL copied to clipboard")

    def _on_toggle_sql(self):
        visible = not self.sql_browser.isVisible()
        self.sql_browser.setVisible(visible)
        self.sql_toggle_btn.setText("Hide SQL" if visible else "Show SQL")

    def _update_sql_panel(self, sql_text):
        """Update the SQL panel content. Collapses it for each new query."""
        if sql_text:
            self.sql_browser.setHtml(
                '<table width="100%" cellpadding="8" cellspacing="0">'
                '<tr><td bgcolor="#2D2D2D">'
                '<code><font color="#F8F8F2">'
                f'{self._escape_html(sql_text)}'
                '</font></code>'
                '</td></tr></table>')
            self.sql_toggle_btn.setEnabled(True)
            self.copy_sql_btn.setEnabled(True)
            # Collapse by default for each new response
            self.sql_browser.setVisible(False)
            self.sql_toggle_btn.setText("Show SQL")
        else:
            self.sql_browser.clear()
            self.sql_browser.setVisible(False)
            self.sql_toggle_btn.setEnabled(False)
            self.sql_toggle_btn.setText("Show SQL")
            self.copy_sql_btn.setEnabled(False)

    # -- Chat formatting ----------------------------------------------------
    #
    # QTextBrowser uses Qt's limited HTML/CSS engine.  Key constraints:
    #   - background / background-color on <div> is ignored
    #   - bgcolor attribute on <td> IS reliable
    #   - <font color="..."> works for text colour
    #   - Markdown from the Genie API must be converted to HTML
    # -----------------------------------------------------------------

    def _render_chat(self):
        """Re-render the full chat from the parts list."""
        self.chat_browser.setHtml(''.join(self._chat_parts))
        self._scroll_chat()

    def _append_chat_user(self, text):
        self._chat_parts.append(
            f'<p><b>You:</b> {self._escape_html(text)}</p>')
        self._render_chat()

    def _append_chat_genie(self, content):
        # Genie response block — grey bg with explicit dark text for dark-mode compat
        body = self._md_to_html(content)
        html = ('<table width="100%" cellpadding="8" cellspacing="0">'
                '<tr><td bgcolor="#E8E8E8">'
                '<font color="#1A1A1A">'
                f'<b>Genie:</b><br/>{body}'
                '</font>'
                '</td></tr></table><br/>')
        self._chat_parts.append(html)
        self._render_chat()

    def _append_chat_error(self, msg):
        self._chat_parts.append(
            f'<p><font color="red"><b>Error:</b> '
            f'{self._escape_html(msg)}</font></p>')
        self._render_chat()

    def _scroll_chat(self):
        sb = self.chat_browser.verticalScrollBar()
        sb.setValue(sb.maximum())

    @staticmethod
    def _escape_html(text):
        """Escape HTML special characters and convert newlines to <br/>."""
        return (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('\n', '<br/>'))

    @staticmethod
    def _md_to_html(text):
        """Convert basic Markdown from Genie responses to Qt-compatible HTML.

        Handles: **bold**, *italic*, ``code``, bullet lists (- item),
        and numbered lists (1. item).
        """
        # Escape HTML first
        text = (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;'))
        # Inline code: `code` → <code>code</code>
        text = re.sub(r'`([^`]+)`', r'<code>\1</code>', text)
        # Bold: **text** → <b>text</b>  (must come before italic)
        text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
        # Italic: *text* → <i>text</i>
        text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
        # Bullet list items:  "- item" at start of line
        text = re.sub(r'^- (.+)$', r'&bull; \1', text, flags=re.MULTILINE)
        # Numbered list items: "1. item" at start of line
        text = re.sub(r'^(\d+)\. (.+)$', r'\1. \2', text, flags=re.MULTILINE)
        # Newlines → <br/>
        text = text.replace('\n', '<br/>')
        return text

    # -- Results table ------------------------------------------------------

    def _populate_results(self, columns, rows):
        """Show up to 100 rows in the preview table."""
        self.results_table.clear()
        if not columns:
            self.results_table.setRowCount(0)
            self.results_table.setColumnCount(0)
            return
        preview = rows[:100]
        self.results_table.setColumnCount(len(columns))
        self.results_table.setHorizontalHeaderLabels(columns)
        self.results_table.setRowCount(len(preview))
        for r, row in enumerate(preview):
            for c, val in enumerate(row):
                text = '' if val is None else str(val)
                # Truncate very long cell values for display
                if len(text) > 200:
                    text = text[:200] + '...'
                self.results_table.setItem(r, c, QTableWidgetItem(text))
        self.results_table.resizeColumnsToContents()

    # -- Geometry column detection ------------------------------------------

    def _populate_geom_combo(self, columns, rows):
        """Populate the geometry column dropdown with auto-detection."""
        self.geom_combo.clear()
        if not columns:
            return

        # Score each column for likelihood of being geometry
        best_idx = -1
        best_score = -1
        for i, col in enumerate(columns):
            score = 0
            # Name heuristic
            if col.lower() in _GEOM_COL_NAMES:
                score += 2
            elif any(n in col.lower() for n in _GEOM_COL_NAMES):
                score += 1
            # Sample value heuristic
            for row in rows[:10]:
                if i < len(row) and row[i] and _is_wkt_format(str(row[i])):
                    score += 3
                    break
            self.geom_combo.addItem(col)
            if score > best_score:
                best_score = score
                best_idx = i

        if best_idx >= 0 and best_score > 0:
            self.geom_combo.setCurrentIndex(best_idx)

    # -- Layer creation (runs on main thread) -------------------------------

    def _on_add_layer(self):
        if not self._current_columns or not self._current_rows:
            QMessageBox.warning(self, "No Data",
                                "No results available to create a layer.")
            return

        geom_col = self.geom_combo.currentText()
        if not geom_col:
            QMessageBox.warning(self, "No Geometry Column",
                                "Please select a geometry column.")
            return

        # Find geometry column index
        geom_idx = None
        for i, c in enumerate(self._current_columns):
            if c.lower() == geom_col.lower():
                geom_idx = i
                break
        if geom_idx is None:
            QMessageBox.warning(self, "Column Not Found",
                                f"Column '{geom_col}' not found in results.")
            return

        # Check if values are WKT
        sample_values = [
            r[geom_idx] for r in self._current_rows[:20]
            if geom_idx < len(r) and r[geom_idx]
        ]
        has_wkt = any(_is_wkt_format(str(v)) for v in sample_values)

        if has_wkt:
            # Path A: create layers directly on main thread (fast, in-memory)
            self._create_layers_from_wkt(
                self._current_columns, self._current_rows,
                geom_idx, f"genie_{geom_col}")
        else:
            # Path B: need to re-query via DB to get WKT — use thread for I/O
            hostname, http_path, token = self._get_connection()
            if not all([hostname, http_path, token]):
                QMessageBox.warning(
                    self, "Missing Connection",
                    "HTTP Path is required for non-WKT geometry re-query. "
                    "Check your connection settings.")
                return
            if not self._current_query:
                QMessageBox.warning(
                    self, "No SQL",
                    "No SQL statement available to re-query.")
                return

            self.add_layer_btn.setEnabled(False)
            self.status_label.setText("Status: Re-querying with ST_ASWKT...")

            self._pending_layer_geom_col = geom_col
            self._requery_thread = GenieReQueryThread(
                hostname, http_path, token,
                self._current_query, geom_col, parent=self)
            self._requery_thread.data_ready.connect(self._on_requery_done)
            self._requery_thread.error_occurred.connect(self._on_layer_error)
            self._requery_thread.status_update.connect(
                lambda s: self.status_label.setText(f"Status: {s}"))
            self._requery_thread.start()

    def _on_requery_done(self, columns, rows):
        """Path B callback — data arrived, create layers on main thread."""
        # The last column is __genie_wkt
        wkt_idx = len(columns) - 1
        self._create_layers_from_wkt(
            columns, rows, wkt_idx,
            f"genie_{self._pending_layer_geom_col}")

    def _create_layers_from_wkt(self, columns, rows, geom_idx, layer_name):
        """Create QGIS memory layers from in-memory data.  MUST run on main thread."""
        self.status_label.setText("Status: Creating layers...")
        QApplication.processEvents()

        # Attribute columns (everything except geometry)
        attr_cols = [(i, c) for i, c in enumerate(columns) if i != geom_idx]

        fields = QgsFields()
        for _, col_name in attr_cols:
            fields.append(QgsField(col_name, QVariant.String))

        # Bucket rows by geometry type
        type_buckets = {}  # e.g. {'Point': [(row, wkt_raw), ...]}
        for row in rows:
            if geom_idx >= len(row) or not row[geom_idx]:
                continue
            wkt_raw = _strip_srid_from_wkt(str(row[geom_idx]))
            geom = QgsGeometry.fromWkt(wkt_raw)
            if geom.isNull() or geom.isEmpty():
                continue
            gtype = geom.type()
            gtype_name = {0: 'Point', 1: 'LineString', 2: 'Polygon'}.get(
                int(gtype), 'Point')
            type_buckets.setdefault(gtype_name, []).append((row, wkt_raw))

        if not type_buckets:
            self.add_layer_btn.setEnabled(True)
            self.status_label.setText("Status: No valid geometries found")
            QMessageBox.warning(
                self, "No Geometries",
                "No valid geometries found in the selected column.")
            return

        layers = []
        for gtype_name, entries in type_buckets.items():
            layer_def = f"{gtype_name}?crs=EPSG:4326"
            lname = (f"{layer_name} ({gtype_name})"
                     if len(type_buckets) > 1 else layer_name)
            mem_layer = QgsVectorLayer(layer_def, lname, "memory")
            prov = mem_layer.dataProvider()
            prov.addAttributes(fields.toList())
            mem_layer.updateFields()

            features = []
            for row, wkt_raw in entries:
                feat = QgsFeature(mem_layer.fields())
                attrs = []
                for idx, _ in attr_cols:
                    val = row[idx] if idx < len(row) else None
                    attrs.append(_coerce_attr(val))
                feat.setAttributes(attrs)
                feat.setGeometry(QgsGeometry.fromWkt(wkt_raw))
                features.append(feat)

            prov.addFeatures(features)
            mem_layer.updateExtents()
            if mem_layer.featureCount() > 0:
                layers.append(mem_layer)

        # Add layers to project
        if layers:
            for lyr in layers:
                QgsProject.instance().addMapLayer(lyr)
            count = sum(lyr.featureCount() for lyr in layers)
            self.status_label.setText(
                f"Status: Added {len(layers)} layer(s) with {count} features")
            QgsMessageLog.logMessage(
                f"Genie: added {len(layers)} layer(s) ({count} features)",
                "Databricks Connector", Qgis.Info)
        else:
            self.status_label.setText("Status: No layers created")

        self.add_layer_btn.setEnabled(True)

    def _on_layer_error(self, msg):
        self.add_layer_btn.setEnabled(True)
        self.status_label.setText("Status: Layer error")
        QMessageBox.warning(self, "Layer Error", msg)
        QgsMessageLog.logMessage(
            f"Genie layer error: {msg}",
            "Databricks Connector", Qgis.Warning)

    # -- Clear chat ---------------------------------------------------------

    def _on_clear_chat(self):
        self._chat_parts.clear()
        self.chat_browser.clear()
        self._update_sql_panel('')
        self.results_table.clear()
        self.results_table.setRowCount(0)
        self.results_table.setColumnCount(0)
        self.geom_combo.clear()
        self.add_layer_btn.setEnabled(False)
        self._conversation_id = None
        self._current_columns = []
        self._current_rows = []
        self._current_query = ''
        self.status_label.setText("Status: Ready")

    # -- Dialog close -------------------------------------------------------

    def closeEvent(self, event):
        """Cancel any running threads on close."""
        self._thinking_timer.stop()
        if self._api_thread and self._api_thread.isRunning():
            self._api_thread.cancel()
            self._api_thread.wait(2000)
        super().closeEvent(event)
