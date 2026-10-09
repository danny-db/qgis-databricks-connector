"""Lakehouse Real-Time (Reyden) SQL warehouses need the Databricks SQL kernel.

Real-Time warehouses don't accept the classic Thrift protocol that
databricks-sql-connector uses by default. Connector 4.6+ notices this and
retries on its Rust "kernel" backend, which ships separately as the optional
``databricks-sql-kernel`` package. These helpers recognise the resulting
error and offer a one-click install, so users who installed the connector
before v1.7.0 can connect without leaving QGIS.
"""

import importlib
import site
import sys

from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtGui import QCursor
from qgis.PyQt.QtWidgets import QApplication, QMessageBox
from qgis.core import Qgis, QgsMessageLog

KERNEL_REQUIREMENT = "databricks-sql-kernel>=1.1.0,<2.0.0"
_LOG_TAG = "Databricks Connector"


def is_kernel_missing(message):
    """True if a connection error means the kernel add-on isn't installed."""
    text = str(message or "")
    return "databricks-sql-kernel" in text and "use_kernel" in text


def kernel_available():
    """True if the kernel can be imported in this QGIS session."""
    importlib.invalidate_caches()
    try:
        importlib.import_module("databricks_sql_kernel")
        return True
    except ImportError:
        return False


def install_kernel():
    """pip-install the kernel into the user site-packages. Returns (ok, detail)."""
    from .databricks_connector import DatabricksConnector

    args = ["install", "--user", KERNEL_REQUIREMENT]
    QgsMessageLog.logMessage(f"Running pip install (in-process): {args}", _LOG_TAG, Qgis.MessageLevel.Info)
    ok, out, err = DatabricksConnector._run_pip(args)
    if out:
        QgsMessageLog.logMessage(f"pip stdout: {out.strip()}", _LOG_TAG, Qgis.MessageLevel.Info)
    if err:
        QgsMessageLog.logMessage(f"pip stderr: {err.strip()}", _LOG_TAG, Qgis.MessageLevel.Warning)
    # A brand-new user site-packages folder isn't on sys.path until Python restarts
    user_site = site.getusersitepackages()
    if ok and user_site not in sys.path:
        site.addsitedir(user_site)
    if ok and kernel_available():
        return True, ""
    return False, "\n".join((err or out or "No output captured").strip().split("\n")[-5:])


def offer_kernel_install(parent, message):
    """If ``message`` is the missing-kernel error, explain it and offer to install.

    Returns True when it handled the message (the caller then skips its own
    error box), False for any other error.
    """
    if not is_kernel_missing(message):
        return False
    reply = QMessageBox.question(
        parent, "Lakehouse Real-Time warehouse",
        "This SQL warehouse uses Lakehouse Real-Time, which connects through the "
        "Databricks SQL kernel, a small add-on to databricks-sql-connector.\n\n"
        "Install it now? It takes a few seconds and doesn't need a QGIS restart.",
        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        QMessageBox.StandardButton.Yes)
    if reply != QMessageBox.StandardButton.Yes:
        return True
    QApplication.setOverrideCursor(QCursor(Qt.CursorShape.WaitCursor))
    try:
        ok, detail = install_kernel()
    finally:
        QApplication.restoreOverrideCursor()
    if ok:
        QMessageBox.information(parent, "Lakehouse Real-Time warehouse",
                                "The Databricks SQL kernel is installed. Please try again.")
    else:
        QMessageBox.warning(
            parent, "Lakehouse Real-Time warehouse",
            f"Couldn't install the Databricks SQL kernel.\n\n{detail}\n\n"
            "You can install it from a terminal with:\n"
            f'  pip install --user "{KERNEL_REQUIREMENT}"\n'
            "using the same Python as QGIS (Python 3.10 or later).")
    return True
