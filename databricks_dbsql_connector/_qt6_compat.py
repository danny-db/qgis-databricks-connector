"""
Qt6 / QGIS 4 compatibility shims.

Qt6 removed unscoped enums. This module patches the old-style attribute
access (e.g. QVariant.String, QMessageBox.Yes, Qt.WindowModal) so that
plugin code written for Qt5/QGIS 3 continues to work on Qt6/QGIS 4.

Import this module early — before any code that references the patched names.
"""

from qgis.PyQt.QtCore import QVariant, Qt

# ── QVariant type constants ──────────────────────────────────────────
if not hasattr(QVariant, 'Type'):
    from qgis.PyQt.QtCore import QMetaType
    QVariant.Type = QMetaType.Type
    QVariant.String = QMetaType.Type.QString
    QVariant.Int = QMetaType.Type.Int
    QVariant.LongLong = QMetaType.Type.LongLong
    QVariant.Double = QMetaType.Type.Double
    QVariant.Bool = QMetaType.Type.Bool
    QVariant.Date = QMetaType.Type.QDate
    QVariant.DateTime = QMetaType.Type.QDateTime

# ── QMessageBox button constants ─────────────────────────────────────
try:
    from qgis.PyQt.QtWidgets import QMessageBox
    if not hasattr(QMessageBox, 'Yes'):
        QMessageBox.Yes = QMessageBox.StandardButton.Yes
        QMessageBox.No = QMessageBox.StandardButton.No
        QMessageBox.Ok = QMessageBox.StandardButton.Ok
        QMessageBox.Cancel = QMessageBox.StandardButton.Cancel
except ImportError:
    pass

# ── Qt enum constants ────────────────────────────────────────────────
if not hasattr(Qt, 'WindowModal'):
    Qt.WindowModal = Qt.WindowModality.WindowModal

if not hasattr(Qt, 'Horizontal'):
    Qt.Horizontal = Qt.Orientation.Horizontal
    Qt.Vertical = Qt.Orientation.Vertical

if not hasattr(Qt, 'UserRole'):
    Qt.UserRole = Qt.ItemDataRole.UserRole

# ── QProcess enums ───────────────────────────────────────────────────
try:
    from qgis.PyQt.QtCore import QProcess
    if not hasattr(QProcess, 'MergedChannels'):
        QProcess.MergedChannels = QProcess.ProcessChannelMode.MergedChannels
    if not hasattr(QProcess, 'NormalExit'):
        QProcess.NormalExit = QProcess.ExitStatus.NormalExit
    if not hasattr(QProcess, 'FailedToStart'):
        QProcess.FailedToStart = QProcess.ProcessError.FailedToStart
        QProcess.Crashed = QProcess.ProcessError.Crashed
        QProcess.Timedout = QProcess.ProcessError.Timedout
        QProcess.WriteError = QProcess.ProcessError.WriteError
        QProcess.ReadError = QProcess.ProcessError.ReadError
        QProcess.UnknownError = QProcess.ProcessError.UnknownError
except ImportError:
    pass

# ── QLineEdit enums ─────────────────────────────────────────────────
try:
    from qgis.PyQt.QtWidgets import QLineEdit
    if not hasattr(QLineEdit, 'Password'):
        QLineEdit.Password = QLineEdit.EchoMode.Password
        QLineEdit.Normal = QLineEdit.EchoMode.Normal
except ImportError:
    pass

# ── QHeaderView enums ───────────────────────────────────────────────
try:
    from qgis.PyQt.QtWidgets import QHeaderView
    if not hasattr(QHeaderView, 'Stretch'):
        QHeaderView.Stretch = QHeaderView.ResizeMode.Stretch
        QHeaderView.Interactive = QHeaderView.ResizeMode.Interactive
        QHeaderView.ResizeToContents = QHeaderView.ResizeMode.ResizeToContents
except ImportError:
    pass
