"""
Main plugin class for Databricks DBSQL Connector - QGIS 3.42 Compatible
"""
import os
import sys
from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication, Qt, QProcess
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
        
        # Installation process (QProcess for proper Qt integration)
        self.install_process = None
        self.progress_dialog = None
        self.install_output = ""

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
    
    def _find_qgis_pip(self):
        """Find pip executable bundled with QGIS.
        
        Returns tuple: (executable, args_list) where executable is either
        pip3 directly or python3, and args_list contains the command arguments.
        """
        import platform
        system = platform.system()
        tried_paths = []
        
        if system == 'Darwin':  # macOS
            # QGIS on macOS: binaries are in Contents/MacOS/bin/
            # sys.executable = /Applications/QGIS.app/Contents/MacOS/QGIS
            qgis_dir = os.path.dirname(sys.executable)
            bin_dir = os.path.join(qgis_dir, 'bin')
            
            # Try pip3 directly first (preferred)
            pip_path = os.path.join(bin_dir, 'pip3')
            tried_paths.append(pip_path)
            if os.path.exists(pip_path):
                return pip_path, ['install', 'databricks-sql-connector'], tried_paths
            
            # Try pip
            pip_path = os.path.join(bin_dir, 'pip')
            tried_paths.append(pip_path)
            if os.path.exists(pip_path):
                return pip_path, ['install', 'databricks-sql-connector'], tried_paths
            
            # Try python3 -m pip
            python_path = os.path.join(bin_dir, 'python3')
            tried_paths.append(python_path)
            if os.path.exists(python_path):
                return python_path, ['-m', 'pip', 'install', 'databricks-sql-connector'], tried_paths
                
        elif system == 'Windows':
            # QGIS on Windows: Python is in apps/PythonXX/
            qgis_root = os.path.dirname(os.path.dirname(sys.executable))
            apps_dir = os.path.join(qgis_root, 'apps')
            
            if os.path.exists(apps_dir):
                for item in os.listdir(apps_dir):
                    if item.lower().startswith('python'):
                        python_dir = os.path.join(apps_dir, item)
                        
                        # Try Scripts/pip.exe
                        pip_path = os.path.join(python_dir, 'Scripts', 'pip.exe')
                        tried_paths.append(pip_path)
                        if os.path.exists(pip_path):
                            return pip_path, ['install', 'databricks-sql-connector'], tried_paths
                        
                        # Try python.exe -m pip
                        python_path = os.path.join(python_dir, 'python.exe')
                        tried_paths.append(python_path)
                        if os.path.exists(python_path):
                            return python_path, ['-m', 'pip', 'install', 'databricks-sql-connector'], tried_paths
        
        else:  # Linux
            import shutil
            # Try pip3 in PATH
            pip_path = shutil.which('pip3')
            if pip_path:
                tried_paths.append(pip_path)
                return pip_path, ['install', 'databricks-sql-connector'], tried_paths
            
            python_path = shutil.which('python3')
            if python_path:
                tried_paths.append(python_path)
                return python_path, ['-m', 'pip', 'install', 'databricks-sql-connector'], tried_paths
        
        return None, None, tried_paths
    
    def _start_installation(self):
        """Start the dependency installation using QProcess (Qt-native, avoids QGIS conflicts)."""
        # Find pip or python executable
        executable, args, tried_paths = self._find_qgis_pip()
        
        # Log what we found
        QgsMessageLog.logMessage(
            f"Searched for pip/python in: {tried_paths}",
            "Databricks Connector",
            Qgis.Info
        )
        
        if executable:
            QgsMessageLog.logMessage(
                f"Found executable: {executable}",
                "Databricks Connector",
                Qgis.Info
            )
        
        # If not found, show manual instructions
        if not executable:
            paths_tried = '\n'.join(tried_paths) if tried_paths else 'No paths found'
            QMessageBox.warning(
                self.iface.mainWindow(),
                "Databricks Connector - Cannot Find pip",
                f"Could not find pip in QGIS installation.\n\n"
                f"Paths searched:\n{paths_tried}\n\n"
                "Please install dependencies manually using the QGIS Python Console:\n\n"
                "1. Open Python Console (Plugins → Python Console)\n"
                "2. Run this code:\n\n"
                "import pip\n"
                "pip.main(['install', 'databricks-sql-connector'])\n\n"
                "3. Restart QGIS"
            )
            return
        
        # Create progress dialog
        self.progress_dialog = QProgressDialog(
            f"Installing databricks-sql-connector...\nThis may take a few minutes.\n\nUsing: {executable}",
            None,  # No cancel button
            0, 0,  # Indeterminate progress
            self.iface.mainWindow()
        )
        self.progress_dialog.setWindowTitle("Databricks Connector - Installing Dependencies")
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.setMinimumDuration(0)
        self.progress_dialog.setMinimumWidth(450)
        self.progress_dialog.show()
        QApplication.processEvents()
        
        # Reset output buffer
        self.install_output = ""
        
        # Create QProcess for pip install
        self.install_process = QProcess(self.iface.mainWindow())
        self.install_process.setProcessChannelMode(QProcess.MergedChannels)
        self.install_process.readyReadStandardOutput.connect(self._on_process_output)
        self.install_process.finished.connect(self._on_process_finished)
        self.install_process.errorOccurred.connect(self._on_process_error)
        
        # Start installation
        self.install_process.start(executable, args)
        
        QgsMessageLog.logMessage(
            f"Starting pip install with: {python_exe} -m pip install databricks-sql-connector",
            "Databricks Connector",
            Qgis.Info
        )
    
    def _on_process_output(self):
        """Capture process output."""
        if self.install_process:
            output = self.install_process.readAllStandardOutput().data().decode('utf-8', errors='replace')
            self.install_output += output
            QgsMessageLog.logMessage(f"pip: {output.strip()}", "Databricks Connector", Qgis.Info)
    
    def _on_process_error(self, error):
        """Handle process error."""
        error_messages = {
            QProcess.FailedToStart: "Failed to start pip process. Python executable may be invalid.",
            QProcess.Crashed: "pip process crashed.",
            QProcess.Timedout: "pip process timed out.",
            QProcess.WriteError: "Error writing to pip process.",
            QProcess.ReadError: "Error reading from pip process.",
            QProcess.UnknownError: "Unknown error occurred."
        }
        error_msg = error_messages.get(error, f"Process error: {error}")
        QgsMessageLog.logMessage(f"Installation error: {error_msg}", "Databricks Connector", Qgis.Warning)
    
    def _on_process_finished(self, exit_code, exit_status):
        """Handle installation completion."""
        # Close progress dialog
        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None
        
        success = (exit_code == 0 and exit_status == QProcess.NormalExit)
        
        if success:
            QMessageBox.information(
                self.iface.mainWindow(),
                "Databricks Connector - Installation Complete",
                "Dependencies installed successfully!\n\n"
                "Please restart QGIS to use the Databricks Connector.\n\n"
                "After restarting, click the Databricks icon to connect."
            )
            QgsMessageLog.logMessage(
                "Dependencies installed successfully. Please restart QGIS.",
                "Databricks Connector",
                Qgis.Success
            )
        else:
            # Get last few lines of output for error message
            error_lines = self.install_output.strip().split('\n')[-5:]
            error_summary = '\n'.join(error_lines) if error_lines else "No output captured"
            
            QMessageBox.warning(
                self.iface.mainWindow(),
                "Databricks Connector - Installation Failed",
                f"Installation failed (exit code: {exit_code}).\n\n"
                f"Last output:\n{error_summary}\n\n"
                "You can try installing manually:\n"
                "1. Open QGIS Python Console (Plugins → Python Console)\n"
                "2. Run: import subprocess, sys\n"
                "3. Run: subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'databricks-sql-connector'])\n"
                "4. Restart QGIS"
            )
            QgsMessageLog.logMessage(
                f"Dependency installation failed. Exit code: {exit_code}. Output: {self.install_output}",
                "Databricks Connector",
                Qgis.Warning
            )
        
        # Clean up
        self.install_process = None
        self.install_output = ""

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
