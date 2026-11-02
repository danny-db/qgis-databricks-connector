"""
Text boxes that grow with their text, for longer questions and prompts.

ChatInput is the question box of Genie Agent, Genie One and Explain this Map:
Enter sends and Shift+Enter starts a new line. GrowingTextEdit is the same box
without Enter-to-send, used for the Explain this Map custom prompt.
"""
import math

from qgis.PyQt.QtCore import QEvent, Qt, pyqtSignal
from qgis.PyQt.QtGui import QTextCursor
from qgis.PyQt.QtWidgets import QPlainTextEdit


class GrowingTextEdit(QPlainTextEdit):
    """Plain-text box that grows from min_lines to max_lines with its text, then scrolls."""

    def __init__(self, parent=None, min_lines=2, max_lines=8):
        super().__init__(parent)
        self._min_lines = min_lines
        self._max_lines = max_lines
        self.document().documentLayout().documentSizeChanged.connect(self._fit_height)
        self._fit_height()

    # A new width, font or style re-wraps the text without a documentSizeChanged.
    def resizeEvent(self, event):
        super().resizeEvent(event)
        if event.size().width() != event.oldSize().width():
            self._fit_height()

    def changeEvent(self, event):
        super().changeEvent(event)
        if event.type() in (QEvent.Type.FontChange, QEvent.Type.StyleChange):
            self._fit_height()

    def showEvent(self, event):
        super().showEvent(event)
        self._fit_height()

    def _fit_height(self, *_):
        lines, line_height = self._laid_out_lines()
        lines = min(max(lines, self._min_lines), self._max_lines)
        frame = self.contentsMargins()      # frame, border and padding, as the style draws them
        margins = self.viewportMargins()
        # +1: QPlainTextEdit shows a scrollbar unless the viewport has a pixel to spare
        text = lines * line_height + 2 * self.document().documentMargin() + 1
        self.setFixedHeight(frame.top() + frame.bottom() + margins.top() + margins.bottom()
                            + math.ceil(text))

    def _laid_out_lines(self):
        """Lines at the current width (up to max_lines), and the height Qt gives a line."""
        lines, line_height = 0, 0.0
        block = self.document().begin()
        while block.isValid() and lines < self._max_lines:
            self.blockBoundingRect(block)   # lays the block out at the current width
            layout = block.layout()
            lines += layout.lineCount()
            if not line_height and layout.lineCount():
                line_height = layout.lineAt(0).height()
            block = block.next()
        return lines, line_height


class ChatInput(GrowingTextEdit):
    """Chat question box: Enter emits ``submitted``, Shift+Enter adds a line."""

    submitted = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTabChangesFocus(True)

    def text(self):
        """The question as plain text, like QLineEdit.text()."""
        return self.toPlainText()

    def clear(self):
        """Empty the box but keep Ctrl+Z, like QLineEdit.clear(), so a sent question can come back."""
        cursor = self.textCursor()
        cursor.select(QTextCursor.SelectionType.Document)
        cursor.removeSelectedText()

    def keyPressEvent(self, event):
        if (event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter)
                and not event.modifiers() & Qt.KeyboardModifier.ShiftModifier):
            event.accept()
            self.submitted.emit()
            return
        super().keyPressEvent(event)
