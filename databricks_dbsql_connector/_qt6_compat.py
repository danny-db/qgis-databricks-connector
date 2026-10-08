"""
Qt5 / Qt6 (QGIS 3 / QGIS 4) compatibility.

The plugin code uses fully scoped enums (e.g. ``Qgis.MessageLevel.Info``,
``QMessageBox.StandardButton.Yes``, ``Qt.WindowModality.WindowModal``), which
work on both PyQt5 and PyQt6, so no enum patching is needed. QGIS 4 also
provides the ``QVariant.String``-style type constants itself.

The only shim left: ``QDialog.exec()`` is used everywhere, but very old PyQt5
builds only have ``exec_()``. Import this module early (see __init__.py).
"""

from qgis.PyQt.QtWidgets import QDialog

if not hasattr(QDialog, 'exec'):
    QDialog.exec = QDialog.exec_
