"""
Main plugin class for Databricks DBSQL Connector - QGIS 3.42 Compatible
"""
import os
import sys
import subprocess
from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication, Qt, QThread, pyqtSignal
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction, QMessageBox, QProgressDialog, QApplication
from qgis.core import (
    QgsApplication,
    QgsProviderRegistry,
    QgsProviderMetadata,
    QgsMessageLog,
    Qgis,
    QgsDataItemProviderRegistry
)

# Check if databricks is available
try:
    from .databricks_provider import DatabricksProvider, DatabricksProviderMetadata
    DATABRICKS_AVAILABLE = True
    IMPORT_ERROR = None
except ImportError as e:
    DATABRICKS_AVAILABLE = False
    IMPORT_ERROR = str(e)

# Browser provider will be imported when needed
BROWSER_AVAILABLE = True  # Assume it's available, import when needed
BROWSER_IMPORT_ERROR = None

from .databricks_dialog import DatabricksDialog


class DependencyInstallThread(QThread):
    """Thread for installing Python dependencies without blocking the UI."""
    
    finished = pyqtSignal(bool, str)  # success, message
    progress = pyqtSignal(str)  # status message
    
    def __init__(self, packages):
        super().__init__()
        self.packages = packages
    
    def run(self):
        """Install the required packages using pip."""
        try:
            self.progress.emit("Installing databricks-sql-connector...")
            
            # Get the Python executable used by QGIS
            python_exe = sys.executable
            
            # Run pip install
            result = subprocess.run(
                [python_exe, '-m', 'pip', 'install'] + self.packages,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                self.finished.emit(True, "Dependencies installed successfully!")
            else:
                error_msg = result.stderr or result.stdout or "Unknown error"
                self.finished.emit(False, f"Installation failed:\n{error_msg}")
                
        except subprocess.TimeoutExpired:
            self.finished.emit(False, "Installation timed out after 5 minutes.")
        except Exception as e:
            self.finished.emit(False, f"Installation error: {str(e)}")


class DatabricksConnector:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :type iface: QgsInterface
        """
        # Save reference to the QGIS interface
        self.iface = iface
        
        # Initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)
        
        # Initialize locale
        locale = QSettings().value('locale/userLocale')[0:2] if QSettings().value('locale/userLocale') else 'en'
        locale_path = os.path.join(
            self.plugin_dir,
            'i18n',
            'DatabricksConnector_{}.qm'.format(locale))

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)
            QCoreApplication.installTranslator(self.translator)

        # Declare instance attributes
        self.actions = []
        self.menu = self.tr('&Databricks DBSQL Connector')
        
        # Check if plugin was started the first time in current QGIS session
        self.first_start = None
        
        # Provider metadata instance
        self.provider_metadata = None
        
        # Browser provider instance
        self.browser_provider = None
        
        # Installation thread
        self.install_thread = None
        self.progress_dialog = None

    def tr(self, message):
        """Get the translation for a string using Qt translation API."""
        return QCoreApplication.translate('DatabricksConnector', message)

    def add_action(
        self,
        icon_path,
        text,
        callback,
        enabled_flag=True,
        add_to_menu=True,
        add_to_toolbar=True,
        status_tip=None,
        whats_this=None,
        parent=None):
        """Add a toolbar icon to the toolbar."""

        icon = QIcon(icon_path) if os.path.exists(icon_path) else QIcon()
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            # Adds plugin icon to Plugins toolbar
            self.iface.addToolBarIcon(action)

        if add_to_menu:
            self.iface.addPluginToMenu(
                self.menu,
                action)

        self.actions.append(action)
        return action

    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""

        icon_path = os.path.join(self.plugin_dir, 'icons', 'databricks.svg')
        
        # Create action to open connection dialog
        self.add_action(
            icon_path,
            text=self.tr('Connect to Databricks SQL'),
            callback=self.run,
            parent=self.iface.mainWindow()
        )

        # Register the custom data provider only if dependencies are available
        if DATABRICKS_AVAILABLE:
            self.register_provider()
        else:
            QgsMessageLog.logMessage(
                f"Databricks connector dependencies not available: {IMPORT_ERROR}",
                "Databricks Connector",
                Qgis.Warning
            )
        
        # Register the browser provider
        if BROWSER_AVAILABLE:
            self.register_browser_provider()
        else:
            QgsMessageLog.logMessage(
                f"Databricks browser provider not available: {BROWSER_IMPORT_ERROR}",
                "Databricks Connector",
                Qgis.Warning
            )
        
        # Will be set False in run()
        self.first_start = True

    def unload(self):
        """Removes the plugin menu item and icon from QGIS GUI."""
        
        # Safely unregister the data provider - compatible with different QGIS versions
        if self.provider_metadata and DATABRICKS_AVAILABLE:
            try:
                registry = QgsProviderRegistry.instance()
                # Try the newer method first
                if hasattr(registry, 'removeProvider'):
                    registry.removeProvider(DatabricksProvider.PROVIDER_KEY)
                # Fall back to older method if available
                elif hasattr(registry, 'unregisterProvider'):
                    registry.unregisterProvider(DatabricksProvider.PROVIDER_KEY)
                else:
                    # For very old versions, just log that we can't unregister
                    QgsMessageLog.logMessage(
                        "Cannot unregister provider - method not available in this QGIS version",
                        "Databricks Connector",
                        Qgis.Warning
                    )
            except Exception as e:
                QgsMessageLog.logMessage(
                    f"Error unregistering provider: {str(e)}",
                    "Databricks Connector",
                    Qgis.Warning
                )
        
        # Unregister the browser provider
        if self.browser_provider and BROWSER_AVAILABLE:
            try:
                # Unregister provider - version compatible
                try:
                    # Try newer QGIS version method first
                    registry = QgsDataItemProviderRegistry.instance()
                except AttributeError:
                    # Fall back to older QGIS version method
                    registry = QgsApplication.dataItemProviderRegistry()
                
                registry.removeProvider(self.browser_provider)
                QgsMessageLog.logMessage(
                    "Databricks browser provider unregistered successfully",
                    "Databricks Connector",
                    Qgis.Info
                )
            except Exception as e:
                QgsMessageLog.logMessage(
                    f"Error unregistering browser provider: {str(e)}",
                    "Databricks Connector",
                    Qgis.Warning
                )
            
        for action in self.actions:
            self.iface.removePluginMenu(
                self.tr('&Databricks DBSQL Connector'),
                action)
            self.iface.removeToolBarIcon(action)

    def register_provider(self):
        """Register the Databricks data provider."""
        try:
            # Create provider metadata
            self.provider_metadata = DatabricksProviderMetadata()
            
            # Register the provider
            registry = QgsProviderRegistry.instance()
            registry.registerProvider(self.provider_metadata)
            
            QgsMessageLog.logMessage(
                "Databricks provider registered successfully",
                "Databricks Connector",
                Qgis.Info
            )
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Failed to register Databricks provider: {str(e)}",
                "Databricks Connector",
                Qgis.Critical
            )
    
    def register_browser_provider(self):
        """Register the Databricks browser provider."""
        try:
            # Import browser provider when needed (within QGIS context)
            from .databricks_browser import DatabricksDataItemProvider
            
            # Create browser provider
            self.browser_provider = DatabricksDataItemProvider()
            
            # Register the provider - version compatible
            try:
                # Try newer QGIS version method first
                registry = QgsDataItemProviderRegistry.instance()
            except AttributeError:
                # Fall back to older QGIS version method
                registry = QgsApplication.dataItemProviderRegistry()
            
            registry.addProvider(self.browser_provider)
            
            QgsMessageLog.logMessage(
                "Databricks browser provider registered successfully",
                "Databricks Connector",
                Qgis.Info
            )
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Failed to register Databricks browser provider: {str(e)}",
                "Databricks Connector",
                Qgis.Critical
            )
            global BROWSER_AVAILABLE, BROWSER_IMPORT_ERROR
            BROWSER_AVAILABLE = False
            BROWSER_IMPORT_ERROR = str(e)

    def install_dependencies(self):
        """Prompt user to install missing dependencies."""
        reply = QMessageBox.question(
            self.iface.mainWindow(),
            "Databricks Connector - Install Dependencies",
            "The Databricks SQL Connector package is required but not installed.\n\n"
            "Would you like to install it now?\n\n"
            "This will run: pip install databricks-sql-connector\n\n"
            "Note: QGIS will need to be restarted after installation.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes
        )
        
        if reply == QMessageBox.Yes:
            self._start_installation()
    
    def _start_installation(self):
        """Start the dependency installation in a background thread."""
        # Create progress dialog
        self.progress_dialog = QProgressDialog(
            "Installing dependencies...",
            "Cancel",
            0, 0,  # Indeterminate progress
            self.iface.mainWindow()
        )
        self.progress_dialog.setWindowTitle("Databricks Connector")
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.setMinimumDuration(0)
        self.progress_dialog.setCancelButton(None)  # Can't cancel pip easily
        self.progress_dialog.show()
        
        # Create and start installation thread
        self.install_thread = DependencyInstallThread(['databricks-sql-connector'])
        self.install_thread.progress.connect(self._on_install_progress)
        self.install_thread.finished.connect(self._on_install_finished)
        self.install_thread.start()
    
    def _on_install_progress(self, message):
        """Update progress dialog with status message."""
        if self.progress_dialog:
            self.progress_dialog.setLabelText(message)
        QApplication.processEvents()
    
    def _on_install_finished(self, success, message):
        """Handle installation completion."""
        # Close progress dialog
        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None
        
        if success:
            QMessageBox.information(
                self.iface.mainWindow(),
                "Databricks Connector - Installation Complete",
                f"{message}\n\n"
                "Please restart QGIS to use the Databricks Connector.\n\n"
                "After restarting, click the Databricks icon to connect."
            )
            QgsMessageLog.logMessage(
                "Dependencies installed successfully. Please restart QGIS.",
                "Databricks Connector",
                Qgis.Success
            )
        else:
            QMessageBox.warning(
                self.iface.mainWindow(),
                "Databricks Connector - Installation Failed",
                f"{message}\n\n"
                "You can try installing manually:\n"
                "1. Open QGIS Python Console (Plugins → Python Console)\n"
                "2. Run: import subprocess, sys\n"
                "3. Run: subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'databricks-sql-connector'])\n"
                "4. Restart QGIS"
            )
            QgsMessageLog.logMessage(
                f"Dependency installation failed: {message}",
                "Databricks Connector",
                Qgis.Warning
            )
        
        # Clean up thread
        self.install_thread = None

    def run(self):
        """Run method that performs all the real work"""
        
        # Check if dependencies are available
        if not DATABRICKS_AVAILABLE:
            self.install_dependencies()
            return
        
        # Create the dialog with elements (after translation) and keep reference
        # Only create GUI ONCE in callback, so that it will only load when the plugin is started
        if self.first_start:
            self.first_start = False
            self.dlg = DatabricksDialog(self.iface)

        # Show the dialog
        result = self.dlg.exec_()
        
        if result:
            # User clicked OK - connection details should be handled in dialog
            pass
