"""
Explain this Map: one click sends the current map to a frontier model on
Databricks (Foundation Model APIs) and streams back an explanation.

The request carries a screenshot of the map canvas plus "map facts" (area,
scale, layers, and what each layer's style encodes) so the model can read the
legend instead of guessing colours. It uses the plugin's saved connection and
sign-in, so there is no API key to set up. Optional knobs: model, prompt
preset or custom prompt, image size, and follow-up questions.
"""
import base64
import os
import tempfile
import time
import urllib.parse

from qgis.PyQt.QtCore import Qt, QThread, QTimer, pyqtSignal, QSettings, QSize
from qgis.PyQt.QtGui import QColor, QImage, QPainter, QPixmap
from qgis.PyQt.QtWidgets import (
    QApplication, QCheckBox, QComboBox, QDialog, QFileDialog, QGroupBox, QHBoxLayout,
    QLabel, QLineEdit, QMessageBox, QPlainTextEdit, QPushButton, QSplitter, QTextBrowser,
    QVBoxLayout,
)
from qgis.core import (
    Qgis, QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsFeatureRequest,
    QgsMapRendererParallelJob, QgsMapSettings,
    QgsMessageLog, QgsProject, QgsRasterLayer, QgsVectorLayer, QgsWkbTypes,
)

from .databricks_auth import AUTH_PAT, get_bearer_token, normalise_auth_method
from . import databricks_ai_core as ai

_SETTINGS = "DatabricksConnector/ExplainMap"
_CONNECTIONS = "DatabricksConnector/Connections"
IMAGE_SIZES = [("Small (1024 px)", 1024), ("Medium (1600 px)", 1600), ("Large (2048 px)", 2048)]
_MAX_CLASSES = 12            # legend entries described per layer
_COUNT_VISIBLE_LIMIT = 50000  # count features in view up to this many (local layers only)


# ---------------------------------------------------------------------------
# Map facts and capture
# ---------------------------------------------------------------------------

def _renderer_facts(layer):
    """What a vector layer's style encodes, in words."""
    renderer = layer.renderer()
    if renderer is None:
        return ""
    kind = renderer.type()
    if kind == "categorizedSymbol":
        cats = renderer.categories()[:_MAX_CLASSES]
        items = ", ".join(f"{c.label() or c.value()} ({c.symbol().color().name()})" for c in cats)
        more = "" if len(renderer.categories()) <= _MAX_CLASSES else f", +{len(renderer.categories()) - _MAX_CLASSES} more"
        return f"categorised by '{renderer.classAttribute()}': {items}{more}"
    if kind == "graduatedSymbol":
        ranges = renderer.ranges()[:_MAX_CLASSES]
        items = ", ".join(f"{r.lowerValue():g}-{r.upperValue():g} ({r.symbol().color().name()})" for r in ranges)
        return f"graduated by '{renderer.classAttribute()}' in {len(renderer.ranges())} classes: {items}"
    if kind == "singleSymbol" and renderer.symbol() is not None:
        return f"single symbol, colour {renderer.symbol().color().name()}"
    if kind == "heatmapRenderer":
        weight = getattr(renderer, "weightExpression", lambda: "")()
        return "heatmap" + (f" weighted by '{weight}'" if weight else "")
    return f"{kind} style"


def _databricks_source(layer):
    """Where a Databricks layer comes from (table or SQL), if known."""
    if layer.providerType() == "databricks":
        params = urllib.parse.parse_qs(urllib.parse.urlparse(layer.source()).query)
        if params.get("sql"):
            sql = " ".join(params["sql"][0].split())
            return f"Databricks SQL query: {sql[:400]}{'...' if len(sql) > 400 else ''}"
        if params.get("table"):
            return f"Databricks table {params['table'][0]}"
    full = layer.customProperty("databricks/full_name", "")
    return f"Databricks table {full}" if full else ""


def collect_map_facts(canvas, include_details=True):
    """Plain-text facts about what the map canvas currently shows."""
    settings = canvas.mapSettings()
    crs = settings.destinationCrs()
    extent = canvas.extent()
    lines = [f"- Coordinate system: {crs.authid() or crs.description()}",
             f"- Scale: about 1:{int(round(canvas.scale())):,}"]
    try:
        wgs84 = QgsCoordinateReferenceSystem("EPSG:4326")
        geo = QgsCoordinateTransform(crs, wgs84, QgsProject.instance()).transformBoundingBox(extent)
        lines.append(f"- Area (lon/lat): west {geo.xMinimum():.4f}, south {geo.yMinimum():.4f}, "
                     f"east {geo.xMaximum():.4f}, north {geo.yMaximum():.4f}")
    except Exception as exc:  # unusual CRS: keep going without lon/lat
        QgsMessageLog.logMessage(f"Explain this Map: no lon/lat extent ({exc})",
                                 "Databricks Connector", Qgis.MessageLevel.Info)

    layers = canvas.layers()
    lines.append(f"- Visible layers (top to bottom): {len(layers)}")
    for layer in layers:
        lines.append(f"  - {_layer_line(layer, extent, crs, include_details)}")
    return "\n".join(lines)


def _layer_line(layer, extent, canvas_crs, include_details):
    if isinstance(layer, QgsVectorLayer):
        gtype = QgsWkbTypes.displayString(layer.wkbType()) if layer.isSpatial() else "table (no geometry)"
        parts = [f"'{layer.name()}': {gtype}, {layer.featureCount():,} features"]
        if include_details:
            visible = _visible_count(layer, extent, canvas_crs)
            if visible is not None:
                parts.append(f"{visible:,} in view")
            if layer.selectedFeatureCount():
                parts.append(f"{layer.selectedFeatureCount():,} selected")
            style = _renderer_facts(layer)
            if style:
                parts.append(f"style: {style}")
            if layer.labelsEnabled() and layer.labeling() is not None:
                field = getattr(layer.labeling().settings(), "fieldName", "") if hasattr(layer.labeling(), "settings") else ""
                parts.append(f"labelled by '{field}'" if field else "labelled")
            source = _databricks_source(layer)
            if source:
                parts.append(source)
        return "; ".join(parts)
    if isinstance(layer, QgsRasterLayer):
        if "type=xyz" in layer.source():
            return f"'{layer.name()}': basemap tiles"
        return f"'{layer.name()}': raster, {layer.width()}x{layer.height()} px"
    return f"'{layer.name()}': {type(layer).__name__.replace('Qgs', '').replace('Layer', ' layer').strip()}"


def _visible_count(layer, extent, canvas_crs):
    """Features within the view for local layers (Databricks layers would need a query)."""
    if layer.providerType() not in ("memory", "ogr", "delimitedtext", "spatialite") or not layer.isSpatial():
        return None
    try:
        rect = QgsCoordinateTransform(canvas_crs, layer.crs(), QgsProject.instance()).transformBoundingBox(extent)
        request = QgsFeatureRequest().setFilterRect(rect).setNoAttributes() \
            .setFlags(QgsFeatureRequest.Flag.NoGeometry).setLimit(_COUNT_VISIBLE_LIMIT)
        return sum(1 for _ in layer.getFeatures(request))
    except Exception:  # counting is a nicety; never block the explanation
        return None


def _render_view(canvas, max_px):
    """Re-render the current view so its longest side is max_px pixels.

    Rendering (rather than grabbing the widget) gives a crisp image that doesn't
    depend on the window size or screen DPI. DPI scales with the pixel size so
    symbols and labels keep their proportions.
    """
    settings = QgsMapSettings(canvas.mapSettings())
    extent = settings.visibleExtent()
    if extent.isEmpty() or extent.height() <= 0:
        return None
    ratio = extent.width() / extent.height()
    width, height = (max_px, max(1, int(round(max_px / ratio)))) if ratio >= 1 \
        else (max(1, int(round(max_px * ratio))), max_px)
    widget = canvas.size()
    base = max(widget.width(), widget.height())
    factor = max_px / base if base >= 200 else max_px / 900.0
    settings.setOutputDpi(settings.outputDpi() * max(0.5, min(factor, 4.0)))
    settings.setOutputSize(QSize(width, height))
    settings.setExtent(extent)
    job = QgsMapRendererParallelJob(settings)
    job.start()
    job.waitForFinished()
    return job.renderedImage()


def _image_bytes(image, fmt, quality=-1):
    """Encode a QImage to bytes (via a temp file, which works the same on Qt5 and Qt6)."""
    fd, path = tempfile.mkstemp(prefix="explain_map_", suffix="." + fmt.lower())
    os.close(fd)
    try:
        image.save(path, fmt, quality)
        with open(path, "rb") as handle:
            return handle.read()
    finally:
        os.remove(path)


def _encode_for_model(image):
    """PNG when it fits the model's image limit, otherwise JPEG.

    Simple maps stay PNG (crisp lines and text). Busy maps, such as a detailed
    basemap at a large size, can be several MB as PNG, so they go as JPEG,
    stepping quality and then size down until the image fits ai.MAX_IMAGE_BYTES.
    """
    data = _image_bytes(image, "PNG")
    if len(data) <= ai.MAX_IMAGE_BYTES:
        return data
    # JPEG has no transparency: flatten onto white so empty areas don't turn black
    flat = QImage(image.size(), QImage.Format.Format_RGB32)
    flat.fill(QColor("white"))
    painter = QPainter(flat)
    painter.drawImage(0, 0, image)
    painter.end()
    for scale in (1.0, 0.75, 0.5):
        scaled = flat if scale == 1.0 else flat.scaled(
            int(flat.width() * scale), int(flat.height() * scale),
            Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
        for quality in (90, 75):
            data = _image_bytes(scaled, "JPEG", quality)
            if len(data) <= ai.MAX_IMAGE_BYTES:
                return data
    return data


def image_extension(data):
    """File extension for captured image bytes."""
    return ".jpg" if data[:3] == b"\xff\xd8\xff" else ".png"


def capture_map_image(canvas, max_px=1600):
    """Image bytes (PNG, or JPEG for busy maps) of the current view, longest side max_px."""
    try:
        image = _render_view(canvas, max_px)
    except Exception as exc:  # fall back to a screenshot of the canvas widget
        QgsMessageLog.logMessage(f"Explain this Map: render failed, using a screenshot ({exc})",
                                 "Databricks Connector", Qgis.MessageLevel.Info)
        image = None
    if image is None or image.isNull():
        image = canvas.grab().toImage()
        if max(image.width(), image.height()) > max_px:
            image = image.scaled(max_px, max_px, Qt.AspectRatioMode.KeepAspectRatio,
                                 Qt.TransformationMode.SmoothTransformation)
    return _encode_for_model(image)


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

def saved_connection_names():
    settings = QSettings()
    settings.beginGroup(_CONNECTIONS)
    names = sorted(settings.childGroups())
    settings.endGroup()
    return names


def connection_config(name):
    settings = QSettings()
    base = f"{_CONNECTIONS}/{name}"
    return {
        "hostname": settings.value(f"{base}/hostname", ""),
        "http_path": settings.value(f"{base}/http_path", ""),
        "access_token": settings.value(f"{base}/access_token", "") or "",
        "auth_method": normalise_auth_method(settings.value(f"{base}/auth_method", AUTH_PAT)),
    }


def default_connection_name():
    names = saved_connection_names()
    last = QSettings().value("DatabricksConnector/LastConnection", "")
    stored = QSettings().value(f"{_SETTINGS}/connection", "")
    for candidate in (stored, last):
        if candidate in names:
            return candidate
    return names[0] if names else ""


# ---------------------------------------------------------------------------
# Background work
# ---------------------------------------------------------------------------

class ModelListThread(QThread):
    """Load the workspace's image-capable models for the picker."""
    loaded = pyqtSignal(list)
    failed = pyqtSignal(str)

    def __init__(self, config, parent=None):
        super().__init__(parent)
        self.config = config

    def run(self):
        try:
            token = get_bearer_token(self.config["hostname"], self.config["http_path"],
                                     self.config["access_token"], self.config["auth_method"])
            self.loaded.emit(ai.list_models(self.config["hostname"], token))
        except Exception as exc:
            self.failed.emit(str(exc))


class ExplainThread(QThread):
    """Stream one answer. Picks a default model first if none is chosen yet."""
    delta = pyqtSignal(str)
    model_chosen = pyqtSignal(str, str)          # endpoint name, display name
    finished_ok = pyqtSignal(str, object, float)  # text, usage, seconds
    failed = pyqtSignal(str)

    def __init__(self, config, model, messages, max_tokens=1200, parent=None):
        super().__init__(parent)
        self.config, self.model, self.messages, self.max_tokens = config, model, messages, max_tokens
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        started = time.time()
        try:
            token = get_bearer_token(self.config["hostname"], self.config["http_path"],
                                     self.config["access_token"], self.config["auth_method"])
            if not token:
                raise ai.ModelError("No credentials: sign in (OAuth) or add an access token to the connection.")
            if not self.model:
                models = ai.list_models(self.config["hostname"], token)
                if not models:
                    raise ai.ModelError("No image-capable models are available on this workspace.")
                self.model = models[0]["name"]
                self.model_chosen.emit(models[0]["name"], models[0]["display_name"])
            text, usage = ai.stream_chat(self.config["hostname"], token, self.model, self.messages,
                                         self.max_tokens, on_delta=self.delta.emit,
                                         should_stop=lambda: self._stop)
            self.finished_ok.emit(text, usage, time.time() - started)
        except Exception as exc:
            self.failed.emit(str(exc))


# ---------------------------------------------------------------------------
# Dialog
# ---------------------------------------------------------------------------

class ExplainMapDialog(QDialog):
    """Explain this Map: one click from the toolbar runs explain() straight away."""

    def __init__(self, iface, parent=None):
        super().__init__(parent)
        self.iface = iface
        self.settings = QSettings()
        self._history = None          # conversation so far (for follow-ups)
        self._transcript = ""         # Markdown shown in the answer panel
        self._live = ""               # text streaming in right now
        self._thread = None
        self._model_thread = None
        self._models = []
        self._image = b""
        self._render_timer = QTimer(self)
        self._render_timer.setInterval(120)
        self._render_timer.timeout.connect(self._render)
        self._setup_ui()
        self._load_connections()

    # -- UI ---------------------------------------------------------------

    def _setup_ui(self):
        self.setWindowTitle("Explain this Map")
        self.resize(960, 680)
        root = QVBoxLayout(self)

        top = QHBoxLayout()
        top.addWidget(QLabel("Connection:"))
        self.conn_combo = QComboBox()
        self.conn_combo.currentIndexChanged.connect(self._on_connection_changed)
        top.addWidget(self.conn_combo, 1)
        top.addWidget(QLabel("Model:"))
        self.model_combo = QComboBox()
        self.model_combo.setMinimumWidth(220)
        self.model_combo.currentIndexChanged.connect(self._on_model_changed)
        top.addWidget(self.model_combo, 1)
        top.addWidget(QLabel("Ask for:"))
        self.preset_combo = QComboBox()
        for pid, label, _prompt in ai.PRESETS:
            self.preset_combo.addItem(label, pid)
        saved_preset = self.settings.value(f"{_SETTINGS}/preset", "explain")
        if saved_preset in ai.PRESET_IDS:
            self.preset_combo.setCurrentIndex(ai.PRESET_IDS.index(saved_preset))
        top.addWidget(self.preset_combo, 1)
        self.explain_btn = QPushButton("Explain")
        self.explain_btn.setToolTip("Capture the map again and explain it")
        self.explain_btn.setDefault(True)
        self.explain_btn.clicked.connect(self.explain)
        top.addWidget(self.explain_btn)
        root.addLayout(top)

        # Optional knobs, collapsed by default
        self.options_box = QGroupBox("Options")
        self.options_box.setCheckable(True)
        self.options_box.setChecked(False)
        opts = QVBoxLayout(self.options_box)
        self.custom_prompt = QPlainTextEdit()
        self.custom_prompt.setPlaceholderText(
            "Custom prompt (optional). Leave empty to use the 'Ask for' preset.\n"
            "Example: Explain this map for a road safety committee in Victoria.")
        self.custom_prompt.setMaximumHeight(70)
        self.custom_prompt.setPlainText(self.settings.value(f"{_SETTINGS}/custom_prompt", ""))
        opts.addWidget(self.custom_prompt)
        row = QHBoxLayout()
        self.details_check = QCheckBox("Include layer and style details")
        self.details_check.setToolTip("Send layer names, feature counts and what each style encodes "
                                      "with the image, so the model can read the legend")
        self.details_check.setChecked(self.settings.value(f"{_SETTINGS}/details", "true") == "true")
        row.addWidget(self.details_check)
        self.size_label = QLabel("Image size:")
        row.addWidget(self.size_label)
        self.size_combo = QComboBox()
        for label, px in IMAGE_SIZES:
            self.size_combo.addItem(label, px)
        self.size_combo.setCurrentIndex(int(self.settings.value(f"{_SETTINGS}/size_index", 1)))
        row.addWidget(self.size_combo)
        row.addStretch()
        reset = QPushButton("Reset options")
        reset.clicked.connect(self._reset_options)
        row.addWidget(reset)
        opts.addLayout(row)
        self._options_inner = [self.custom_prompt, self.details_check, self.size_label, self.size_combo, reset]
        self.options_box.toggled.connect(self._on_options_toggled)
        self._on_options_toggled(False)
        root.addWidget(self.options_box)

        split = QSplitter(Qt.Orientation.Horizontal)
        self.thumb = QLabel("The map will appear here")
        self.thumb.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.thumb.setMinimumWidth(260)
        split.addWidget(self.thumb)
        self.answer = QTextBrowser()
        self.answer.setOpenExternalLinks(True)
        split.addWidget(self.answer)
        split.setSizes([300, 640])
        root.addWidget(split, 1)

        follow = QHBoxLayout()
        self.follow_edit = QLineEdit()
        self.follow_edit.setPlaceholderText("Ask a follow-up about this map...")
        self.follow_edit.returnPressed.connect(self.ask_follow_up)
        follow.addWidget(self.follow_edit, 1)
        self.follow_btn = QPushButton("Ask")
        self.follow_btn.clicked.connect(self.ask_follow_up)
        follow.addWidget(self.follow_btn)
        root.addLayout(follow)

        bottom = QHBoxLayout()
        self.status = QLabel("")
        self.status.setWordWrap(True)
        bottom.addWidget(self.status, 1)
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.clicked.connect(self._stop)
        self.stop_btn.setVisible(False)
        bottom.addWidget(self.stop_btn)
        self.copy_btn = QPushButton("Copy")
        self.copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(self._transcript))
        bottom.addWidget(self.copy_btn)
        self.save_btn = QPushButton("Save...")
        self.save_btn.clicked.connect(self._save)
        bottom.addWidget(self.save_btn)
        root.addLayout(bottom)
        self._set_busy(False)

    def _on_options_toggled(self, on):
        for widget in self._options_inner:
            widget.setVisible(on)

    def _reset_options(self):
        self.custom_prompt.clear()
        self.details_check.setChecked(True)
        self.size_combo.setCurrentIndex(1)

    # -- Connection + models ----------------------------------------------

    def _load_connections(self):
        self.conn_combo.blockSignals(True)
        self.conn_combo.clear()
        for name in saved_connection_names():
            self.conn_combo.addItem(name)
        default = default_connection_name()
        if default:
            self.conn_combo.setCurrentIndex(self.conn_combo.findText(default))
        self.conn_combo.blockSignals(False)
        self._on_connection_changed()

    def _config(self):
        name = self.conn_combo.currentText()
        return connection_config(name) if name else None

    def _on_connection_changed(self, *_):
        name = self.conn_combo.currentText()
        if not name:
            return
        self.settings.setValue(f"{_SETTINGS}/connection", name)
        cached = self.settings.value(f"{_SETTINGS}/models/{name}", "")
        self._fill_models([m.split("|", 1) for m in cached.split(";;") if "|" in m]
                          if cached else [], keep_loading=True)
        self._refresh_models()

    def _refresh_models(self):
        config = self._config()
        if not config or (self._model_thread and self._model_thread.isRunning()):
            return
        self._model_thread = ModelListThread(config, self)
        self._model_thread.loaded.connect(self._on_models_loaded)
        self._model_thread.failed.connect(
            lambda msg: self.status.setText(f"Couldn't list models: {msg}") if not self._models else None)
        self._model_thread.start()

    def _on_models_loaded(self, models):
        pairs = [[m["name"], m["display_name"]] for m in models]
        self.settings.setValue(f"{_SETTINGS}/models/{self.conn_combo.currentText()}",
                               ";;".join(f"{n}|{d}" for n, d in pairs))
        self._fill_models(pairs)

    def _fill_models(self, pairs, keep_loading=False):
        self._models = pairs
        wanted = self.settings.value(f"{_SETTINGS}/model", "")
        self.model_combo.blockSignals(True)
        self.model_combo.clear()
        for name, display in pairs:
            self.model_combo.addItem(display, name)
        if not pairs:
            self.model_combo.addItem("Loading models..." if keep_loading else "(none available)", "")
        idx = self.model_combo.findData(wanted)
        self.model_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self.model_combo.blockSignals(False)

    def _on_model_changed(self, *_):
        name = self.model_combo.currentData()
        if name:
            self.settings.setValue(f"{_SETTINGS}/model", name)

    def _model(self):
        return self.model_combo.currentData() or self.settings.value(f"{_SETTINGS}/model", "")

    # -- Explain ------------------------------------------------------------

    def _prompt(self):
        custom = self.custom_prompt.toPlainText().strip()
        return custom or ai.preset_prompt(self.preset_combo.currentData())

    def _remember(self):
        self.settings.setValue(f"{_SETTINGS}/preset", self.preset_combo.currentData())
        self.settings.setValue(f"{_SETTINGS}/custom_prompt", self.custom_prompt.toPlainText())
        self.settings.setValue(f"{_SETTINGS}/details", "true" if self.details_check.isChecked() else "false")
        self.settings.setValue(f"{_SETTINGS}/size_index", self.size_combo.currentIndex())

    def explain(self):
        """Capture the map and explain it (what the toolbar button triggers)."""
        if self._thread and self._thread.isRunning():
            return
        config = self._config()
        if not config:
            QMessageBox.information(self, "Explain this Map",
                                    "Save a Databricks connection first (Plugins → Databricks DBSQL "
                                    "Connector → Connect to Databricks SQL).")
            return
        self._remember()
        canvas = self.iface.mapCanvas()
        self._image = capture_map_image(canvas, self.size_combo.currentData())
        pix = QPixmap()
        pix.loadFromData(self._image)
        self.thumb.setPixmap(pix.scaled(280, 280, Qt.AspectRatioMode.KeepAspectRatio,
                                        Qt.TransformationMode.SmoothTransformation))
        facts = collect_map_facts(canvas, self.details_check.isChecked())
        self._last_facts = facts
        self._history = ai.build_messages(self._prompt(), facts,
                                          base64.b64encode(self._image).decode())
        title = self.preset_combo.currentText() if not self.custom_prompt.toPlainText().strip() else "Custom prompt"
        self._transcript = f"**{title}**\n\n"
        self._start(config)

    def ask_follow_up(self):
        question = self.follow_edit.text().strip()
        if not question or not self._history or (self._thread and self._thread.isRunning()):
            return
        self.follow_edit.clear()
        self._history = ai.build_messages(None, None, None, history=self._history, follow_up=question)
        self._transcript += f"\n\n---\n\n**You:** {question}\n\n"
        self._start(self._config())

    def _start(self, config):
        self._live = ""
        self._set_busy(True)
        self.status.setText("Thinking... (sending the map to your Databricks workspace)")
        self._thread = ExplainThread(config, self._model(), self._history, parent=self)
        self._thread.delta.connect(self._on_delta)
        self._thread.model_chosen.connect(self._on_model_chosen)
        self._thread.finished_ok.connect(self._on_done)
        self._thread.failed.connect(self._on_failed)
        self._thread.start()
        self._render_timer.start()

    def _on_model_chosen(self, name, display):
        self.settings.setValue(f"{_SETTINGS}/model", name)
        if self.model_combo.findData(name) < 0:
            self._fill_models([[name, display]] + [p for p in self._models if p[0] != name])
        self.model_combo.setCurrentIndex(self.model_combo.findData(name))

    def _on_delta(self, piece):
        self._live += piece

    def _on_done(self, text, usage, seconds):
        self._render_timer.stop()
        self._history.append({"role": "assistant", "content": text})
        self._transcript += text
        self._live = ""
        self._render()
        tokens = (usage or {}).get("total_tokens")
        self.status.setText(" · ".join(filter(None, [
            self.model_combo.currentText(),
            f"{tokens:,} tokens" if tokens else "",
            f"{seconds:.1f} s",
            "answered on your Databricks workspace"])))
        self._set_busy(False)

    def _on_failed(self, message):
        self._render_timer.stop()
        self._live = ""
        if self._history and self._history[-1].get("role") == "user" and len(self._history) > 2:
            self._history.pop()            # let the user retry the follow-up
        self._transcript += f"\n\n> ⚠️ {message}\n"
        self._render()
        self.status.setText(message)
        self._set_busy(False)

    def _stop(self):
        if self._thread and self._thread.isRunning():
            self._thread.stop()

    def _render(self):
        markdown = self._transcript + self._live
        if hasattr(self.answer, "setMarkdown"):
            self.answer.setMarkdown(markdown)
        else:
            self.answer.setPlainText(markdown)
        bar = self.answer.verticalScrollBar()
        bar.setValue(bar.maximum())

    def _set_busy(self, busy):
        self.explain_btn.setEnabled(not busy)
        self.follow_btn.setEnabled(not busy and bool(self._history))
        self.follow_edit.setEnabled(not busy and bool(self._history))
        self.stop_btn.setVisible(busy)

    def _save(self):
        target, _ = QFileDialog.getSaveFileName(self, "Save explanation", "map_explanation.md",
                                                "Markdown (*.md)")
        if target:
            self.save_to(target)

    def save_to(self, target):
        """Write the explanation (Markdown) and the captured map image side by side."""
        if not target.lower().endswith(".md"):
            target += ".md"
        image_path = os.path.splitext(target)[0] + image_extension(self._image)
        with open(image_path, "wb") as handle:
            handle.write(self._image)
        with open(target, "w", encoding="utf-8") as handle:
            handle.write(f"![Map]({os.path.basename(image_path)})\n\n{self._transcript}\n")
        self.status.setText(f"Saved {target}")
        return target

    def closeEvent(self, event):
        self._stop()
        if self._thread:
            self._thread.wait(3000)
        super().closeEvent(event)
