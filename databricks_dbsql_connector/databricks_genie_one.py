"""
Databricks Genie One — ask natural-language questions across the whole
workspace (no Genie Agent to pick) via the Genie One MCP server, with results
preview and layer creation reused from the Genie Agent dialog.

Genie One is exposed as a managed MCP server behind Unity Gateway:

    POST https://<workspace>/ai-gateway/mcp-services/system.ai.genie_one_mcp

We speak just enough MCP (JSON-RPC 2.0 over streamable HTTP) to call its
tools: ``view_ask`` -> ``genie_poll_response`` (until completed) ->
``genie_get_query_result`` for each query Genie ran -> ``view_poll_response``
for the charts. Authentication goes through databricks_auth, so both Personal
Access Tokens and OAuth sign-in work.

Charts: ``genie_ask`` answers in text only. ``view_ask`` (offered to MCP Apps
clients, which is how we initialise) makes Genie One draw visualisations, and
``view_poll_response`` returns each one's AI/BI ``renderSpec`` plus the
statement it plots. We draw that spec natively (databricks_genie_charts) and
place it where Genie's answer embeds ``![Visualization](#viz_...)``.
"""
import json
import re
import time
import urllib.error
import urllib.request

from qgis.PyQt.QtCore import QThread, pyqtSignal
from qgis.PyQt.QtWidgets import QComboBox, QHBoxLayout, QLabel

from qgis.core import Qgis, QgsMessageLog

from .databricks_auth import AUTH_PAT, get_bearer_token
from .databricks_genie import GenieDialog, _https_url, auto_chart
from .databricks_genie_charts import render_spec

# Unity Gateway endpoint for Genie One. The older Beta endpoint
# (/api/2.0/mcp/genie) is deprecated and sunsets on 31 October 2026.
GENIE_ONE_MCP_PATH = "/ai-gateway/mcp-services/system.ai.genie_one_mcp"

_MCP_PROTOCOL_VERSION = "2025-06-18"

_RATE_LIMIT_RETRIES = 4
_RATE_LIMIT_BACKOFF_SECS = 5


# ---------------------------------------------------------------------------
# Minimal MCP client (JSON-RPC over streamable HTTP)
# ---------------------------------------------------------------------------

class GenieOneClient:
    """Tiny MCP client for the Genie One server: initialise once, call tools."""

    def __init__(self, hostname, access_token, timeout=120):
        self.url = _https_url(hostname, GENIE_ONE_MCP_PATH)
        self.access_token = access_token
        self.timeout = timeout
        self.session_id = None
        self._next_id = 0

    def _post(self, payload):
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/event-stream',
            'MCP-Protocol-Version': _MCP_PROTOCOL_VERSION,
        }
        if self.session_id:
            headers['Mcp-Session-Id'] = self.session_id
        req = urllib.request.Request(
            self.url, data=json.dumps(payload).encode('utf-8'),
            headers=headers, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # nosec B310 - https only (_https_url)
                self.session_id = (resp.headers.get('Mcp-Session-Id')
                                   or self.session_id)
                ctype = resp.headers.get('Content-Type') or ''
                raw = resp.read().decode('utf-8')
        except urllib.error.HTTPError as exc:
            body = ''
            try:
                body = exc.read().decode('utf-8', errors='replace')[:300]
            except (OSError, ValueError):
                body = ''   # no readable error body; status code still reported
            if exc.code in (401, 403):
                raise RuntimeError(
                    f"Genie One authentication failed ({exc.code}). Sign in "
                    "again (OAuth) or check that your token can use Genie One."
                    f"\n{body}")
            if exc.code == 404:
                raise RuntimeError(
                    "Genie One is not available on this workspace (404 from "
                    f"{GENIE_ONE_MCP_PATH}).")
            if exc.code == 429:
                raise RuntimeError(
                    "Rate limited (429). Please wait a moment and try again.")
            raise RuntimeError(f"HTTP {exc.code}: {exc.reason}\n{body}")

        if 'id' not in payload:          # notification: no response body
            return None
        if 'text/event-stream' in ctype:
            # Streamable HTTP may answer with SSE; the reply is the 'data:'
            # event carrying our request id.
            for line in raw.splitlines():
                if line.startswith('data:'):
                    msg = json.loads(line[5:].strip())
                    if msg.get('id') == payload['id']:
                        return msg
            raise RuntimeError("Genie One returned no response for the request.")
        return json.loads(raw)

    def _request(self, method, params=None):
        # Unity Catalog rate-limits busy orgs (REQUEST_LIMIT_EXCEEDED / 429);
        # it is transient, so back off and retry a few times.
        for attempt in range(_RATE_LIMIT_RETRIES + 1):
            self._next_id += 1
            try:
                msg = self._post({'jsonrpc': '2.0', 'id': self._next_id,
                                  'method': method, 'params': params or {}})
            except RuntimeError as exc:
                if '429' in str(exc) and attempt < _RATE_LIMIT_RETRIES:
                    time.sleep(_RATE_LIMIT_BACKOFF_SECS * (attempt + 1))
                    continue
                raise
            err = msg.get('error')
            if err and 'REQUEST_LIMIT_EXCEEDED' in str(err.get('message', '')) \
                    and attempt < _RATE_LIMIT_RETRIES:
                time.sleep(_RATE_LIMIT_BACKOFF_SECS * (attempt + 1))
                continue
            if err:
                raise RuntimeError(f"Genie One error: {err.get('message', err)}")
            return msg.get('result', {})

    def open(self):
        """Run the MCP initialise handshake as an MCP Apps-capable client,
        so the server offers the visualising ``view_*`` tools."""
        self._request('initialize', {
            'protocolVersion': _MCP_PROTOCOL_VERSION,
            'capabilities': {'extensions': {'io.modelcontextprotocol/ui': {
                'mimeTypes': ['text/html;profile=mcp-app']}}},
            'clientInfo': {'name': 'qgis-databricks-connector', 'version': '1'},
        })
        self._post({'jsonrpc': '2.0', 'method': 'notifications/initialized'})

    def call_tool(self, name, arguments):
        """Call an MCP tool and return its ``structuredContent`` dict."""
        result = self._request('tools/call',
                               {'name': name, 'arguments': arguments})
        text = ' '.join(c.get('text', '') for c in result.get('content', [])
                        if c.get('type') == 'text')
        if result.get('isError'):
            raise RuntimeError(f"Genie One {name} failed: {text[:500]}")
        structured = result.get('structuredContent')
        if structured is None:
            try:
                structured = json.loads(text)
            except ValueError:
                structured = {'text': text}
        return structured


def _step_text(step):
    """One-line summary of a Genie One progress step (dict or string)."""
    if isinstance(step, dict):
        for key in ('summary', 'title', 'text', 'description', 'label', 'name'):
            if step.get(key):
                step = step[key]
                break
    text = re.sub(r'<!--.*?-->', ' ', str(step), flags=re.S)
    text = text.split('|')[0]                  # drop markdown table bodies
    text = re.sub(r'[*`#]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:90] + ('...' if len(text) > 90 else '')


# ---------------------------------------------------------------------------
# QThread: one Genie One question (ask / poll / fetch results)
# ---------------------------------------------------------------------------

class GenieOneApiThread(QThread):
    """Drive a single Genie One question and emit the same result dict as
    GenieApiThread, plus ``deep_link`` and every query's ``results``."""

    response_received = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)
    status_update = pyqtSignal(str)

    _MAX_POLL_SECS = 600   # 10 min
    _POLL_INTERVAL = 2.0
    _NOT_FOUND_GRACE_SECS = 60
    _VIZ_SETTLE_TRIES = 6       # chart specs can land a few seconds late

    def __init__(self, hostname, access_token, question,
                 conversation_id=None, parent=None,
                 http_path=None, auth_method=AUTH_PAT):
        super().__init__(parent)
        self.hostname = hostname
        self.access_token = access_token
        self.http_path = http_path
        self.auth_method = auth_method
        self.question = question
        self.conversation_id = conversation_id
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def _fetch_result(self, client, conversation_id, response_id, item):
        """Fetch one query's rows, waiting briefly if it is still running."""
        for _ in range(30):
            res = client.call_tool('genie_get_query_result', {
                'conversation_id': conversation_id,
                'response_id': response_id,
                'item_id': item['item_id'],
            })
            if res.get('ready', True):
                break
            time.sleep(1.0)
        return {
            'item_id': item['item_id'],
            'sql': item.get('sql', ''),
            'columns': [c.get('name', f'col_{i}')
                        for i, c in enumerate(res.get('columns') or [])],
            'rows': res.get('rows') or [],
            'total_row_count': res.get('total_row_count'),
            'truncated': bool(res.get('truncated')),
        }

    def run(self):
        try:
            token = get_bearer_token(self.hostname, self.http_path,
                                     self.access_token, self.auth_method)
            if not token:
                raise RuntimeError(
                    "No credentials available. Sign in (OAuth) or set an "
                    "access token on this connection.")

            client = GenieOneClient(self.hostname, token)
            self.status_update.emit("Connecting to Genie One")
            client.open()

            # 1. Ask (optionally continuing the current conversation)
            args = {'question': self.question}
            if self.conversation_id:
                args['conversation_id'] = self.conversation_id
            self.status_update.emit("Sending question to Genie One")
            try:
                # view_ask makes Genie One produce charts; genie_ask is text-only
                ask = client.call_tool('view_ask', args)
            except RuntimeError as exc:
                if 'view_ask' not in str(exc) and 'not found' not in str(exc).lower():
                    raise
                ask = client.call_tool('genie_ask', args)
            conversation_id = ask.get('conversation_id')
            response_id = ask.get('response_id')
            if not (conversation_id and response_id):
                raise RuntimeError(
                    f"Unexpected genie_ask response: {str(ask)[:300]}")

            # 2. Poll until Genie One finishes, surfacing its progress steps
            self.status_update.emit("Genie One is thinking")
            started = time.time()
            while True:
                if self._cancelled:
                    try:
                        client.call_tool('genie_cancel_response',
                                         {'conversation_id': conversation_id})
                    except Exception as exc:
                        # Best effort: the local request is cancelled anyway
                        QgsMessageLog.logMessage(
                            f"Genie One cancel request failed: {exc}",
                            "Databricks Connector", Qgis.MessageLevel.Info)
                    raise RuntimeError("Cancelled by user.")
                try:
                    poll = client.call_tool('genie_poll_response', {
                        'conversation_id': conversation_id,
                        'response_id': response_id,
                    })
                except RuntimeError as exc:
                    # A follow-up's response id can take ~10 s to register;
                    # until then polling reports "not found". Keep waiting.
                    if ('not found' in str(exc)
                            and time.time() - started < self._NOT_FOUND_GRACE_SECS):
                        time.sleep(self._POLL_INTERVAL)
                        continue
                    raise
                status = (poll.get('status') or '').lower()
                steps = poll.get('progress_steps') or []
                if steps:
                    self.status_update.emit(_step_text(steps[-1]))
                if status == 'completed':
                    break
                if status != 'in_progress':
                    detail = poll.get('error') or poll.get('final_answer') or status
                    raise RuntimeError(f"Genie One returned '{status}': {detail}")
                if time.time() - started > self._MAX_POLL_SECS:
                    raise RuntimeError("Genie One timed out after 10 minutes.")
                time.sleep(self._POLL_INTERVAL)

            # 3. Fetch rows for every query Genie One ran
            results = []
            items = poll.get('query_items') or []
            for n, item in enumerate(items, 1):
                if self._cancelled:
                    raise RuntimeError("Cancelled by user.")
                self.status_update.emit(
                    f"Fetching query results ({n}/{len(items)})")
                results.append(self._fetch_result(
                    client, conversation_id, response_id, item))

            # The primary result is the last query that returned rows.
            primary = next((r for r in reversed(results) if r['rows']),
                           results[-1] if results else None)

            # 4. Charts Genie One drew, placed where the answer embeds them
            content = poll.get('final_answer') or ''
            charts, content = self._charts(client, conversation_id,
                                           response_id, content, results)
            if not charts and primary and primary['rows']:
                chart = auto_chart(primary['columns'], primary['rows'])
                if chart:
                    charts.append(chart)

            self.response_received.emit({
                'conversation_id': conversation_id,
                'message_id': response_id,
                'charts': charts,
                'content': content,
                'deep_link': poll.get('deep_link') or '',
                'query_statement': primary['sql'] if primary else '',
                'columns': primary['columns'] if primary else [],
                'rows': primary['rows'] if primary else [],
                'results': results,
                'primary_index': results.index(primary) if primary else -1,
            })
        except Exception as exc:
            self.error_occurred.emit(str(exc))

    def _charts(self, client, conversation_id, response_id, content, results):
        """Render Genie One's visualisations; returns (charts, content) with
        each ``![...](#viz_id)`` embed replaced by a ``[[chart:N]]`` marker."""
        wanted = set(re.findall(r'!\[[^\]]*\]\(#(viz_[A-Za-z0-9]+)\)', content))
        specs = {}
        for attempt in range(self._VIZ_SETTLE_TRIES):
            try:
                view_status, specs = self._viz_specs(
                    client, conversation_id, response_id)
            except Exception as exc:   # view tools unavailable: no charts
                QgsMessageLog.logMessage(f"Genie One charts unavailable: {exc}",
                                         "Databricks Connector", Qgis.MessageLevel.Info)
                break
            # The view stream can lag the final answer by a few seconds:
            # wait until it is complete and holds every embedded chart.
            if view_status == 'completed' and wanted.issubset(specs):
                break
            self.status_update.emit("Fetching charts")
            time.sleep(self._POLL_INTERVAL)

        by_item = {r['item_id']: r for r in results}
        charts = []
        for embed_id, viz in specs.items():
            res = by_item.get(viz['item_id'])
            png = None
            if res:
                try:
                    png = render_spec(viz['spec'], res['columns'], res['rows'],
                                      title=viz['title'])
                except Exception as exc:
                    QgsMessageLog.logMessage(
                        f"Genie One chart '{viz['title']}' failed: {exc}",
                        "Databricks Connector", Qgis.MessageLevel.Warning)
            if not png:
                continue
            marker = f'[[chart:{len(charts)}]]'
            charts.append({'title': viz['title'], 'png': png,
                           'embed_id': embed_id})
            content = re.sub(r'!\[[^\]]*\]\(#' + re.escape(embed_id) + r'\)',
                             '\n' + marker + '\n', content)
        # Drop embeds we could not draw (e.g. map/heatmap widgets)
        content = re.sub(r'!\[[^\]]*\]\(#viz_[A-Za-z0-9]+\)', '', content)
        return charts, content

    @staticmethod
    def _viz_specs(client, conversation_id, response_id):
        """(view status, {embed_id: {spec, title, item_id}}) from the view
        item stream."""
        view = client.call_tool('view_poll_response', {
            'conversation_id': conversation_id, 'response_id': response_id})
        items = []
        for raw in view.get('items_json') or []:
            try:
                items.append(json.loads(raw) if isinstance(raw, str) else raw)
            except ValueError:
                continue
        # QUERY_EXECUTION output id == genie_poll_response query item id
        stmt_to_item = {}
        for it in items:
            md = it.get('metadata') or {}
            if it.get('type') == 'function_call_output' and \
                    md.get('ui_type') == 'QUERY_EXECUTION' and md.get('statement_id'):
                stmt_to_item[md['statement_id']] = it.get('id')
        specs = {}
        for it in items:
            md = it.get('metadata') or {}
            if it.get('type') != 'function_call_output' or not md.get('viz_definition'):
                continue
            try:
                definition = json.loads(md['viz_definition'])
            except ValueError:
                continue
            spec = definition.get('renderSpec') or definition
            embed_id = md.get('embed_id') or md.get('asset_id')
            specs[embed_id] = {
                'spec': spec,
                'title': ((spec.get('frame') or {}).get('title')
                          or (definition.get('frame') or {}).get('title') or 'Chart'),
                'item_id': stmt_to_item.get(md.get('statement_id')),
            }
        return (view.get('status') or '').lower(), specs


# ---------------------------------------------------------------------------
# Dialog
# ---------------------------------------------------------------------------

class GenieOneDialog(GenieDialog):
    """Genie One chat: same chat, SQL, results and Add-as-Layer experience as
    the Genie Agent dialog, but questions go to Genie One across the whole
    workspace, so there is no Genie Agent to choose."""

    def __init__(self, iface, parent=None):
        self._results = []
        super().__init__(iface, parent)

    # -- UI -----------------------------------------------------------------

    def _setup_ui(self):
        super()._setup_ui()
        self.setWindowTitle("Databricks Genie One")

        # No Genie Agent picker: Genie One finds the right data itself.
        self.space_label.setVisible(False)
        self.space_combo.setVisible(False)
        self.chat_browser.setOpenExternalLinks(True)
        self.question_edit.setPlaceholderText(
            "Ask Genie One anything about your data across the workspace...")

        # Result picker (Genie One can run several queries per answer) and a
        # link to continue the conversation in Databricks.
        row = QHBoxLayout()
        row.addWidget(QLabel("Result:"))
        self.result_combo = QComboBox()
        self.result_combo.setMinimumWidth(320)
        self.result_combo.setEnabled(False)
        self.result_combo.currentIndexChanged.connect(self._on_result_selected)
        row.addWidget(self.result_combo, stretch=1)
        self.deep_link_label = QLabel("")
        self.deep_link_label.setOpenExternalLinks(True)
        row.addWidget(self.deep_link_label)
        root = self.layout()
        root.insertLayout(root.indexOf(self.results_table), row)

    # -- Hooks from GenieDialog ---------------------------------------------

    def _on_connection_changed(self, _index):
        """Genie One needs no Genie Agent list; just reflect readiness."""
        hostname, _http_path, token, auth_method = self._get_connection()
        ready = self._creds_ready(hostname, token, auth_method)
        self.status_label.setText(
            "Status: Ready" if ready else "Status: Select a saved connection")

    def _ready_to_ask(self):
        return True

    def _create_api_thread(self, hostname, http_path, token, auth_method,
                           question):
        return GenieOneApiThread(
            hostname, token, question,
            conversation_id=self._conversation_id, parent=self,
            http_path=http_path, auth_method=auth_method)

    # -- Responses ----------------------------------------------------------

    def _on_response(self, result):
        super()._on_response(result)
        self._results = result.get('results') or []

        self.result_combo.blockSignals(True)
        self.result_combo.clear()
        for n, res in enumerate(self._results, 1):
            sql_line = ' '.join(res['sql'].split())
            label = f"{n}. {len(res['rows'])} rows: {sql_line}"
            self.result_combo.addItem(
                label[:110] + ('...' if len(label) > 110 else ''))
        primary = result.get('primary_index', -1)
        if primary >= 0:
            self.result_combo.setCurrentIndex(primary)
        self.result_combo.setEnabled(len(self._results) > 1)
        self.result_combo.blockSignals(False)

        link = result.get('deep_link')
        self.deep_link_label.setText(
            f'<a href="{link}">Open in Databricks</a>' if link else '')

    def _on_result_selected(self, index):
        """Switch the table, SQL panel and geometry picker to another query."""
        if not (0 <= index < len(self._results)):
            return
        res = self._results[index]
        self._current_columns = res['columns']
        self._current_rows = res['rows']
        self._current_query = res['sql']
        self._update_sql_panel(res['sql'])
        self._populate_results(res['columns'], res['rows'])
        self._populate_geom_combo(res['columns'], res['rows'])
        self.add_layer_btn.setEnabled(len(res['rows']) > 0)

    def _on_clear_chat(self):
        super()._on_clear_chat()
        self._results = []
        self.result_combo.clear()
        self.result_combo.setEnabled(False)
        self.deep_link_label.setText('')

    # -- Markdown -----------------------------------------------------------

    @staticmethod
    def _md_to_html(text):
        """Genie One answers also use headings and links; add those on top of
        the Genie Agent markdown conversion."""
        html = GenieDialog._md_to_html(text)
        # Headings: "## Title" at the start of a line -> bold line
        html = re.sub(r'(^|<br/>)#{1,6} ([^<]+)', r'\1<b>\2</b>', html)
        # Links: [label](https://...) -> clickable anchor
        html = re.sub(r'\[([^\]]+)\]\((https?://[^)\s]+)\)',
                      r'<a href="\2">\1</a>', html)
        return html
