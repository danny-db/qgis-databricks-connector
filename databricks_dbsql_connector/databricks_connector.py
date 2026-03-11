"""
Main plugin class for Databricks DBSQL Connector - QGIS 3.42 Compatible
"""
import os
import sys
from qgis.PyQt.QtCore import QSettings, QTranslator, QCoreApplication, Qt, QProcess, QDate, QTime, QDateTime, QVariant
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction, QMessageBox, QProgressDialog, QApplication
from qgis.core import (
    QgsApplication,
    QgsProviderRegistry,
    QgsProviderMetadata,
    QgsMessageLog,
    Qgis,
    QgsDataItemProviderRegistry,
    QgsFeature,
    QgsGeometry
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
        
        # Create action to refresh selected Databricks layer
        refresh_icon_path = os.path.join(self.plugin_dir, 'icons', 'databricks.svg')
        self.add_action(
            refresh_icon_path,
            text=self.tr('Update Layer Data from Databricks'),
            callback=self.refresh_selected_layer,
            add_to_toolbar=False,  # Only in menu
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
            "This will install: databricks-sql-connector\n\n"
            "Note: QGIS will need to be restarted after installation.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes
        )

        if reply == QMessageBox.Yes:
            self._start_installation()

    @staticmethod
    def _run_pip(pip_args):
        """Run a pip command in-process, following the enmap-box pattern.

        Calls pip's internal API directly so the installation works even when
        the bundled Python binary cannot run standalone (QGIS 4 macOS).
        Captures stdout/stderr to avoid polluting the QGIS Python console.

        Returns (success: bool, stdout: str, stderr: str).
        """
        from io import StringIO
        _orig_out, _orig_err = sys.stdout, sys.stderr
        sys.stdout = StringIO()
        sys.stderr = StringIO()
        success = False
        msg_out = msg_err = None
        try:
            from pip._internal.cli.main_parser import parse_command
            from pip._internal.commands import create_command

            cmd_name, cmd_args = parse_command(pip_args)
            cmd = create_command(cmd_name, isolated=("--isolated" in cmd_args))
            result = cmd.main(cmd_args)
            msg_out = sys.stdout.getvalue()
            msg_err = sys.stderr.getvalue()
            success = (result == 0)
        except Exception as ex:
            msg_err = str(ex)
        finally:
            sys.stdout = _orig_out
            sys.stderr = _orig_err
        return success, (msg_out or ""), (msg_err or "")

    def _start_installation(self):
        """Install dependencies using pip from within the running Python process.

        QGIS 4 on macOS bundles Python as an embedded interpreter whose standalone
        binary cannot bootstrap itself (missing encodings module, wrong sys.prefix).
        Spawning an external QProcess therefore fails.  Instead we call pip's
        internal API directly — the same pattern used by the enmap-box plugin —
        which runs inside the already-working QGIS Python environment.

        Uses ``--user`` to install into the Python user site-packages directory,
        which is on sys.path in both QGIS 3 and 4.
        """
        # Show progress
        self.progress_dialog = QProgressDialog(
            "Installing databricks-sql-connector...\nThis may take a minute.",
            None, 0, 0,
            self.iface.mainWindow()
        )
        self.progress_dialog.setWindowTitle("Databricks Connector - Installing Dependencies")
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.setMinimumDuration(0)
        self.progress_dialog.setMinimumWidth(400)
        self.progress_dialog.show()
        QApplication.processEvents()

        pip_args = ['install', '--user', 'databricks-sql-connector']
        QgsMessageLog.logMessage(
            f"Running pip install (in-process): {pip_args}",
            "Databricks Connector", Qgis.Info
        )

        success, msg_out, msg_err = self._run_pip(pip_args)

        if msg_out:
            QgsMessageLog.logMessage(f"pip stdout: {msg_out.strip()}", "Databricks Connector", Qgis.Info)
        if msg_err:
            QgsMessageLog.logMessage(f"pip stderr: {msg_err.strip()}", "Databricks Connector", Qgis.Warning)

        # Close progress dialog
        if self.progress_dialog:
            self.progress_dialog.close()
            self.progress_dialog = None

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
                "Databricks Connector", Qgis.Success
            )
        else:
            error_lines = msg_err.strip().split('\n')[-5:] if msg_err else ["No output captured"]
            error_summary = '\n'.join(error_lines)
            QMessageBox.warning(
                self.iface.mainWindow(),
                "Databricks Connector - Installation Failed",
                f"Installation failed.\n\n{error_summary}\n\n"
                "You can try manually in the QGIS Python Console:\n\n"
                "from pip._internal.cli.main_parser import parse_command\n"
                "from pip._internal.commands import create_command\n"
                "cmd_name, cmd_args = parse_command(['install', '--user', 'databricks-sql-connector'])\n"
                "create_command(cmd_name).main(cmd_args)"
            )

    def refresh_selected_layer(self):
        """Refresh all selected Databricks layers with fresh data from the database"""
        from qgis.core import QgsProject, QgsVectorLayer
        
        # Get all selected layers from layer tree
        selected_layers = self.iface.layerTreeView().selectedLayers()
        
        if not selected_layers:
            QMessageBox.warning(
                self.iface.mainWindow(),
                "No Layer Selected",
                "Please select one or more Databricks layers to refresh."
            )
            return
        
        # Filter to only Databricks vector layers
        databricks_layers = []
        for layer in selected_layers:
            if isinstance(layer, QgsVectorLayer):
                is_databricks = layer.customProperty("databricks/is_databricks_layer", "false") == "true"
                QgsMessageLog.logMessage(
                    f"Layer '{layer.name()}': is_databricks={is_databricks}",
                    "Databricks Connector",
                    Qgis.Info
                )
                if is_databricks:
                    databricks_layers.append(layer)
                else:
                    QgsMessageLog.logMessage(
                        f"Layer '{layer.name()}' skipped - not a Databricks layer",
                        "Databricks Connector",
                        Qgis.Warning
                    )
        
        if not databricks_layers:
            QMessageBox.warning(
                self.iface.mainWindow(),
                "No Databricks Layers",
                "None of the selected layers are Databricks layers.\n\n"
                "Only layers created by the Databricks connector can be refreshed."
            )
            return
        
        # Build confirmation message
        layer_names = [layer.name() for layer in databricks_layers]
        if len(layer_names) == 1:
            confirm_msg = f"Refresh layer '{layer_names[0]}' with current data from Databricks?"
        else:
            confirm_msg = f"Refresh {len(layer_names)} layers with current data from Databricks?\n\n"
            confirm_msg += "Layers:\n" + "\n".join(f"  • {name}" for name in layer_names)
        
        # Confirm refresh
        reply = QMessageBox.question(
            self.iface.mainWindow(),
            "Refresh Layers",
            confirm_msg,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes
        )
        
        if reply != QMessageBox.Yes:
            return
        
        # Refresh each layer
        for layer in databricks_layers:
            hostname = layer.customProperty("databricks/hostname", "")
            http_path = layer.customProperty("databricks/http_path", "")
            access_token = layer.customProperty("databricks/access_token", "")
            full_name = layer.customProperty("databricks/full_name", "")
            geometry_column = layer.customProperty("databricks/geometry_column", "")
            max_features_str = layer.customProperty("databricks/max_features", "0")
            
            if not all([hostname, http_path, access_token, full_name]):
                QgsMessageLog.logMessage(
                    f"Skipping layer '{layer.name()}' - missing connection info",
                    "Databricks Connector",
                    Qgis.Warning
                )
                continue
            
            try:
                max_features = int(max_features_str)
            except ValueError:
                max_features = 0
            
            # Perform refresh
            self._do_refresh_layer(layer, hostname, http_path, access_token, 
                                   full_name, geometry_column, max_features)
    
    def _do_refresh_layer(self, layer, hostname, http_path, access_token, 
                          full_name, geometry_column, max_features):
        """Actually perform the layer refresh operation"""
        try:
            if not DATABRICKS_AVAILABLE:
                QMessageBox.critical(
                    self.iface.mainWindow(),
                    "Databricks Not Available",
                    "The Databricks SQL connector is not installed."
                )
                return
            
            from databricks import sql
            
            # Show progress dialog
            progress = QProgressDialog(
                "Refreshing layer from Databricks...",
                None,
                0, 0,
                self.iface.mainWindow()
            )
            progress.setWindowTitle("Databricks Connector - Refresh Layer")
            progress.setWindowModality(Qt.WindowModal)
            progress.setMinimumDuration(0)
            progress.show()
            QApplication.processEvents()
            
            # Connect to Databricks
            connection = sql.connect(
                server_hostname=hostname,
                http_path=http_path,
                access_token=access_token
            )
            
            # Escape identifiers helper
            def escape_id(identifier):
                if not identifier:
                    return identifier
                return f"`{identifier.strip('`')}`"
            
            # Build escaped table reference
            parts = full_name.split('.')
            escaped_table_ref = '.'.join(escape_id(p) for p in parts)
            escaped_geom_col = escape_id(geometry_column)
            
            # Build query based on layer's current fields
            # Note: Table/column identifiers cannot be parameterized in SQL.
            # Security is ensured via escape_id() which wraps identifiers in backticks.
            field_names = [escape_id(f.name()) for f in layer.fields()]
            select_clause = field_names.copy()
            select_clause.append(f"ST_ASWKT({escaped_geom_col}) as geom_wkt")
            
            query = f"SELECT {', '.join(select_clause)} FROM {escaped_table_ref}"
            if max_features > 0:
                query += f" LIMIT {max_features}"
            
            QgsMessageLog.logMessage(
                f"Refreshing layer with query: {query}",
                "Databricks Connector",
                Qgis.Info
            )
            
            # Execute query
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
            
            connection.close()
            
            progress.setLabelText(f"Updating layer with {len(rows)} features...")
            QApplication.processEvents()
            
            # Disable map canvas rendering during update to avoid blank canvas
            self.iface.mapCanvas().freeze(True)
            
            try:
                # Start editing to clear and add features
                layer.startEditing()
                
                # Delete all existing features
                feature_ids = [f.id() for f in layer.getFeatures()]
                layer.deleteFeatures(feature_ids)
                
                # Add new features
                features_added = 0
                layer_fields = layer.fields()
                
                for i, row in enumerate(rows):
                    try:
                        feature = QgsFeature(layer_fields)
                        
                        # Set attributes with proper type conversion (consistent with browser/dialog)
                        raw_attrs = list(row[:-1])
                        processed_attrs = []
                        
                        for j, attr_value in enumerate(raw_attrs):
                            if j < len(layer_fields):
                                field = layer_fields[j]
                                field_type = field.type()
                                
                                if attr_value is None:
                                    processed_attrs.append(None)
                                elif field_type == QVariant.LongLong:
                                    processed_attrs.append(int(attr_value) if attr_value is not None else None)
                                elif field_type == QVariant.Double:
                                    processed_attrs.append(float(attr_value) if attr_value is not None else None)
                                elif field_type == QVariant.DateTime:
                                    # Convert Python datetime to QDateTime
                                    if hasattr(attr_value, 'year'):
                                        qdate = QDate(attr_value.year, attr_value.month, attr_value.day)
                                        qtime = QTime(attr_value.hour, attr_value.minute, attr_value.second,
                                                      attr_value.microsecond // 1000 if hasattr(attr_value, 'microsecond') else 0)
                                        processed_attrs.append(QDateTime(qdate, qtime))
                                    else:
                                        processed_attrs.append(None)
                                elif field_type == QVariant.Date:
                                    if hasattr(attr_value, 'year'):
                                        processed_attrs.append(QDate(attr_value.year, attr_value.month, attr_value.day))
                                    else:
                                        processed_attrs.append(None)
                                elif field_type == QVariant.String:
                                    processed_attrs.append(str(attr_value) if attr_value is not None else None)
                                else:
                                    processed_attrs.append(attr_value)
                        
                        feature.setAttributes(processed_attrs)
                        
                        # Set geometry
                        geom_wkt = row[-1] if row else None
                        if geom_wkt and geom_wkt.strip():
                            geometry = QgsGeometry.fromWkt(str(geom_wkt))
                            if not geometry.isNull():
                                feature.setGeometry(geometry)
                                layer.addFeature(feature)
                                features_added += 1
                    except Exception as e:
                        QgsMessageLog.logMessage(
                            f"Error adding feature {i}: {str(e)}",
                            "Databricks Connector",
                            Qgis.Warning
                        )
                
                # Commit changes
                layer.commitChanges()
                layer.updateExtents()
                
                # Ensure editing is off
                if layer.isEditable():
                    layer.rollBack()
                    
            finally:
                # Re-enable map canvas rendering
                self.iface.mapCanvas().freeze(False)
            
            # Refresh the canvas with new data
            self.iface.mapCanvas().refresh()
            
            progress.close()
            
            QgsMessageLog.logMessage(
                f"Refreshed layer '{layer.name()}' with {features_added} features",
                "Databricks Connector",
                Qgis.Info
            )
            
        except Exception as e:
            QMessageBox.critical(
                self.iface.mainWindow(),
                "Refresh Failed",
                f"Failed to refresh layer: {str(e)}"
            )
            QgsMessageLog.logMessage(
                f"Error refreshing layer: {str(e)}",
                "Databricks Connector",
                Qgis.Critical
            )
            
            # Roll back any partial changes
            if layer.isEditable():
                layer.rollBack()

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
        result = self.dlg.exec()
        
        if result:
            # User clicked OK - connection details should be handled in dialog
            pass
