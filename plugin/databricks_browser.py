"""
Databricks Browser Provider for QGIS Browser Panel
"""
import os
from typing import List, Dict, Any, Optional
from qgis.PyQt.QtCore import QThread, pyqtSignal, QSettings
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QAction, QMenu, QMessageBox, QInputDialog
from qgis.core import (
    QgsDataItem, QgsDataItemProvider, QgsDataProvider,
    QgsDataCollectionItem, QgsLayerItem, QgsDataSourceUri,
    QgsMessageLog, Qgis, QgsErrorItem, QgsProject,
    QgsVectorLayer, QgsProviderRegistry, QgsApplication
)

# Check if databricks is available
try:
    from databricks import sql
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

from .databricks_dialog import DatabricksQueryDialog


class DatabricksConnectionItem(QgsDataCollectionItem):
    """Root item for a Databricks connection"""
    
    def __init__(self, parent, name, connection_config):
        super().__init__(parent, name, "/Databricks/" + name)
        
        self.connection_config = connection_config
        # Use custom connection icon
        icon_path = os.path.join(os.path.dirname(__file__), 'icons', 'connection.svg')
        if os.path.exists(icon_path):
            self.setIcon(QIcon(icon_path))
        else:
            self.setIcon(QgsApplication.getThemeIcon('/mIconConnect.svg'))
    
    def capabilities(self):
        """Return item capabilities"""
        return QgsDataItem.Fertile
        
    def createChildren(self):
        """Create catalog children"""
        if not DATABRICKS_AVAILABLE:
            error_item = QgsErrorItem(self, "Databricks connector not available", 
                                    "/Databricks/" + self.name() + "/error")
            return [error_item]
        
        try:
            catalogs = self._get_catalogs()
            children = []
            
            for catalog in catalogs:
                catalog_item = DatabricksCatalogItem(self, catalog, self.connection_config)
                children.append(catalog_item)
            
            # Add custom query option
            query_item = DatabricksQueryItem(self, "Custom Query", self.connection_config)
            children.append(query_item)
            
            return children
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error creating Databricks children: {str(e)}",
                "Databricks Browser",
                Qgis.Critical
            )
            error_item = QgsErrorItem(self, f"Error: {str(e)}", 
                                    "/Databricks/" + self.name() + "/error")
            return [error_item]
    
    def _get_catalogs(self):
        """Get list of accessible catalogs using information_schema (same as custom query dialog)"""
        try:
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            with connection.cursor() as cursor:
                # Use information_schema like the working custom query dialog
                info_query = """
                    SELECT DISTINCT table_catalog
                    FROM information_schema.columns 
                    WHERE table_catalog IS NOT NULL 
                    ORDER BY table_catalog
                """
                
                QgsMessageLog.logMessage(
                    f"Browser: Querying accessible catalogs with: {info_query}",
                    "Databricks Browser",
                    Qgis.Info
                )
                
                cursor.execute(info_query)
                results = cursor.fetchall()
                catalogs = [row[0] for row in results if row[0]]  # Filter out None values
                
                QgsMessageLog.logMessage(
                    f"Browser: Found {len(catalogs)} accessible catalogs: {catalogs}",
                    "Databricks Browser",
                    Qgis.Info
                )
            
            connection.close()
            return catalogs
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error getting catalogs from information_schema: {str(e)}",
                "Databricks Browser",
                Qgis.Warning
            )
            return []
    
    def actions(self, parent):
        """Return context menu actions"""
        actions = []
        
        # Custom query action
        query_action = QAction("Execute Custom Query...", parent)
        query_action.triggered.connect(self._execute_custom_query)
        actions.append(query_action)
        
        # Refresh action
        refresh_action = QAction("Refresh", parent)
        refresh_action.triggered.connect(self.refresh)
        actions.append(refresh_action)
        
        return actions
    
    def _execute_custom_query(self):
        """Open custom query dialog"""
        try:
            dialog = DatabricksQueryDialog(
                self.connection_config,
                QgsApplication.instance().activeWindow()
            )
            dialog.exec_()
        except Exception as e:
            QMessageBox.critical(
                QgsApplication.instance().activeWindow(),
                "Error",
                f"Failed to open query dialog: {str(e)}"
            )


class DatabricksCatalogItem(QgsDataCollectionItem):
    """Item representing a Databricks catalog"""
    
    def __init__(self, parent, catalog_name, connection_config):
        super().__init__(parent, catalog_name, parent.path() + "/" + catalog_name)
        self.catalog_name = catalog_name
        self.connection_config = connection_config
        self.setIcon(QgsApplication.getThemeIcon('/mIconDbSchema.svg'))
    
    def capabilities(self):
        """Return item capabilities"""
        return QgsDataItem.Fertile
    
    def createChildren(self):
        """Create schema children"""
        try:
            schemas = self._get_schemas()
            children = []
            
            for schema in schemas:
                schema_item = DatabricksSchemaItem(self, schema, self.catalog_name, self.connection_config)
                children.append(schema_item)
            
            return children
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error creating catalog children: {str(e)}",
                "Databricks Browser",
                Qgis.Critical
            )
            error_item = QgsErrorItem(self, f"Error: {str(e)}", self.path() + "/error")
            return [error_item]
    
    def _get_schemas(self):
        """Get list of schemas in this catalog using information_schema"""
        try:
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            with connection.cursor() as cursor:
                # Use information_schema like the working custom query dialog
                info_query = f"""
                    SELECT DISTINCT table_schema
                    FROM information_schema.columns 
                    WHERE table_catalog = '{self.catalog_name}'
                        AND table_schema IS NOT NULL 
                    ORDER BY table_schema
                """
                
                cursor.execute(info_query)
                results = cursor.fetchall()
                schemas = [row[0] for row in results if row[0]]
            
            connection.close()
            return schemas
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error getting schemas from information_schema: {str(e)}",
                "Databricks Browser",
                Qgis.Warning
            )
            return []


class DatabricksSchemaItem(QgsDataCollectionItem):
    """Item representing a Databricks schema"""
    
    def __init__(self, parent, schema_name, catalog_name, connection_config):
        super().__init__(parent, schema_name, parent.path() + "/" + schema_name)
        self.schema_name = schema_name
        self.catalog_name = catalog_name
        self.connection_config = connection_config
        self.setIcon(QgsApplication.getThemeIcon('/mIconFolder.svg'))
    
    def capabilities(self):
        """Return item capabilities"""
        return QgsDataItem.Fertile
    
    def createChildren(self):
        """Create table children"""
        try:
            tables = self._get_tables()
            children = []
            
            for table_info in tables:
                table_item = DatabricksTableItem(
                    self, 
                    table_info['table_name'],
                    self.catalog_name,
                    self.schema_name,
                    table_info,
                    self.connection_config
                )
                children.append(table_item)
            
            return children
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error creating schema children: {str(e)}",
                "Databricks Browser",
                Qgis.Critical
            )
            error_item = QgsErrorItem(self, f"Error: {str(e)}", self.path() + "/error")
            return [error_item]
    
    def _get_tables(self):
        """Get list of tables in this schema with geometry information using information_schema (same as custom query dialog)"""
        try:
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            tables = {}  # Use dict to group columns by table
            
            with connection.cursor() as cursor:
                # Use information_schema like the working custom query dialog
                info_query = f"""
                    SELECT DISTINCT table_name, column_name, data_type
                    FROM information_schema.columns 
                    WHERE table_catalog = '{self.catalog_name}'
                        AND table_schema = '{self.schema_name}'
                        AND table_name IS NOT NULL 
                    ORDER BY table_name, column_name
                """
                
                cursor.execute(info_query)
                results = cursor.fetchall()
                
                # Group results by table and identify geometry columns
                for row in results:
                    table_name = row[0]
                    column_name = row[1]
                    data_type = row[2]
                    
                    if table_name not in tables:
                        tables[table_name] = {
                            'table_name': table_name,
                            'geometry_column': None,
                            'geometry_type': None,
                            'has_geometry': False
                        }
                    
                    # Check if this is a geometry column
                    if data_type and data_type.upper() in ['GEOGRAPHY', 'GEOMETRY']:
                        tables[table_name]['geometry_column'] = column_name
                        tables[table_name]['geometry_type'] = data_type
                        tables[table_name]['has_geometry'] = True
            
            connection.close()
            return list(tables.values())
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error getting tables from information_schema: {str(e)}",
                "Databricks Browser",
                Qgis.Warning
            )
            return []


class DatabricksTableItem(QgsDataCollectionItem):
    """Item representing a Databricks table - expandable to show schema"""
    
    def __init__(self, parent, table_name, catalog_name, schema_name, table_info, connection_config):
        super().__init__(parent, table_name, parent.path() + "/" + table_name)
        
        self.table_name = table_name
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_info = table_info
        self.connection_config = connection_config
        
        # Set appropriate icon using QGIS default icons
        if table_info['has_geometry']:
            # Use standard QGIS vector layer icon
            self.setIcon(QgsApplication.getThemeIcon('/mIconGeometryEditVertexTool.svg'))
        else:
            # Use standard QGIS table icon  
            self.setIcon(QgsApplication.getThemeIcon('/mIconTable.svg'))
    
    def capabilities(self):
        """Return item capabilities"""
        return QgsDataItem.Fertile  # Allow expansion to show schema
    
    def createChildren(self):
        """Create children items showing table schema (columns)"""
        children = []
        
        try:
            # Get table schema from Databricks
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            with connection.cursor() as cursor:
                table_ref = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
                cursor.execute(f"DESCRIBE {table_ref}")
                schema_info = cursor.fetchall()
                
                for row in schema_info:
                    col_name = row[0]
                    col_type = row[1]
                    col_comment = row[2] if len(row) > 2 else ""
                    
                    # Create column item
                    display_name = f"{col_name} ({col_type})"
                    if col_comment:
                        display_name += f" - {col_comment}"
                    
                    # Determine if this is the geometry column
                    is_geometry = (col_name == self.table_info.get('geometry_column') or 
                                 col_type.upper() in ['GEOMETRY', 'GEOGRAPHY'])
                    
                    column_item = DatabricksColumnItem(
                        self, display_name, col_name, col_type, is_geometry
                    )
                    children.append(column_item)
            
            connection.close()
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error getting table schema: {str(e)}",
                "Databricks Browser",
                Qgis.Warning
            )
            # Add error item
            error_item = QgsErrorItem(self, f"Error loading schema: {str(e)}", self.path() + "/error")
            children.append(error_item)
        
        return children
    
    def actions(self, parent):
        """Return context menu actions"""
        actions = []
        
        if self.table_info['has_geometry']:
            # Add as layer action
            add_layer_action = QAction("Add Layer", parent)
            add_layer_action.triggered.connect(self._add_layer)
            actions.append(add_layer_action)
            
            # Set as default action for double-click
            add_layer_action.setData("default")
        
        # View data action (for both geometry and non-geometry tables)
        view_action = QAction("View Data...", parent)
        view_action.triggered.connect(self._view_data)
        actions.append(view_action)
        
        return actions
    
    def handleDoubleClick(self):
        """Handle double-click on table item"""
        if self.table_info['has_geometry']:
            self._add_layer()
            return True
        return False
    
    def _add_layer(self):
        """Add this table as a layer to QGIS using simplified synchronous approach"""
        try:
            if not DATABRICKS_AVAILABLE:
                QgsMessageLog.logMessage(
                    "databricks-sql-connector not installed",
                    "Databricks Browser",
                    Qgis.Critical
                )
                return
            
            from qgis.core import QgsProject, QgsVectorLayer, QgsFields, QgsField, QgsFeature, QgsGeometry, QgsWkbTypes
            from PyQt5.QtCore import QVariant
            import databricks.sql as sql
            
            layer_name = f"{self.catalog_name}_{self.schema_name}_{self.table_name}"
            
            QgsMessageLog.logMessage(
                f"Loading table: {layer_name}",
                "Databricks Browser",
                Qgis.Info
            )
            
            # Connect to Databricks
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            with connection.cursor() as cursor:
                # Get table schema - exclude geometry column from attributes
                table_ref = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
                cursor.execute(f"DESCRIBE {table_ref}")
                schema_info = cursor.fetchall()
                
                # Build QGIS fields - exclude geometry column
                fields = QgsFields()
                geometry_column = self.table_info['geometry_column']
                
                for row in schema_info:
                    col_name = row[0]
                    col_type = row[1].upper()
                    
                    # Skip geometry column and geometry types
                    if col_name.lower() != geometry_column.lower() and not col_type.startswith(('GEOGRAPHY', 'GEOMETRY')):
                        # Map databricks types to QGIS types
                        if 'STRING' in col_type or 'VARCHAR' in col_type:
                            fields.append(QgsField(col_name, QVariant.String))
                        elif 'INT' in col_type or 'BIGINT' in col_type:
                            fields.append(QgsField(col_name, QVariant.LongLong))
                        elif 'DOUBLE' in col_type or 'FLOAT' in col_type or 'DECIMAL' in col_type:
                            fields.append(QgsField(col_name, QVariant.Double))
                        elif 'DATE' in col_type:
                            fields.append(QgsField(col_name, QVariant.Date))
                        elif 'TIMESTAMP' in col_type:
                            fields.append(QgsField(col_name, QVariant.DateTime))
                        else:
                            fields.append(QgsField(col_name, QVariant.String))
                
                # Query data with geometry conversion
                geometry_sql = f"ST_ASWKT({geometry_column})" if geometry_column else "NULL"
                
                # Get attribute columns (excluding geometry)
                attr_columns = [f.name() for f in fields]
                attr_sql = ", ".join(attr_columns) if attr_columns else "1"
                
                data_query = f"""
                    SELECT {attr_sql}, {geometry_sql} as geometry_wkt
                    FROM {table_ref}
                    LIMIT 1000
                """
                
                cursor.execute(data_query)
                rows = cursor.fetchall()
                
            connection.close()
            
            # Process rows and detect geometry types
            geometry_types = {}  # geometry_type -> [rows]
            
            for row in rows:
                try:
                    # Get WKT geometry (last column)
                    wkt_geom = row[-1] if row else None
                    if wkt_geom and wkt_geom != 'NULL':
                        # Strip SRID prefix if present
                        clean_wkt = self._strip_srid_from_wkt(str(wkt_geom))
                        
                        # Detect geometry type from WKT
                        geom = QgsGeometry.fromWkt(clean_wkt)
                        if not geom.isEmpty():
                            geom_type_name = QgsWkbTypes.displayString(geom.wkbType()).upper()
                            
                            # Group by base geometry type (POINT, LINESTRING, POLYGON)
                            if 'POINT' in geom_type_name:
                                base_type = 'Point'
                            elif 'LINESTRING' in geom_type_name:
                                base_type = 'LineString'
                            elif 'POLYGON' in geom_type_name:
                                base_type = 'Polygon'
                            else:
                                base_type = 'Unknown'
                            
                            if base_type not in geometry_types:
                                geometry_types[base_type] = []
                            geometry_types[base_type].append(row)
                            
                except Exception as e:
                    QgsMessageLog.logMessage(
                        f"Error processing geometry in row: {str(e)}",
                        "Databricks Browser",
                        Qgis.Warning
                    )
            
            # Create separate layers for each geometry type
            layers_created = 0
            for geom_type, type_rows in geometry_types.items():
                if self._create_geometry_layer(layer_name, geom_type, type_rows, fields):
                    layers_created += 1
            
            if layers_created > 0:
                QgsMessageLog.logMessage(
                    f"Successfully created {layers_created} layers for table: {layer_name}",
                    "Databricks Browser",
                    Qgis.Info
                )
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error adding layer: {str(e)}",
                "Databricks Browser",
                Qgis.Critical
            )
    
    def _strip_srid_from_wkt(self, wkt_str):
        """Strip SRID prefix from WKT string"""
        if not isinstance(wkt_str, str):
            return wkt_str
        
        wkt_str = wkt_str.strip()
        
        if wkt_str.upper().startswith('SRID='):
            srid_parts = wkt_str.split(';', 1)
            if len(srid_parts) > 1:
                return srid_parts[1].strip()
        
        return wkt_str
    
    def _create_geometry_layer(self, base_layer_name, geom_type, rows, fields):
        """Create a memory layer for a specific geometry type"""
        try:
            from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsWkbTypes, QgsProject
            
            layer_name = f"{base_layer_name}_{geom_type}"
            memory_layer = QgsVectorLayer(f"{geom_type}?crs=EPSG:4326", layer_name, "memory")
            
            if not memory_layer.isValid():
                return False
            
            memory_layer.startEditing()
            provider = memory_layer.dataProvider()
            provider.addAttributes(fields.toList())
            memory_layer.updateFields()
            
            features_added = 0
            for row in rows:
                try:
                    feature = QgsFeature()
                    attributes = list(row[:-1]) if len(row) > 1 else []
                    feature.setAttributes(attributes)
                    
                    wkt_geom = row[-1] if row else None
                    if wkt_geom and wkt_geom != 'NULL':
                        clean_wkt = self._strip_srid_from_wkt(str(wkt_geom))
                        geom = QgsGeometry.fromWkt(clean_wkt)
                        
                        if not geom.isEmpty():
                            feature.setGeometry(geom)
                            if memory_layer.addFeature(feature):
                                features_added += 1
                except:
                    pass
            
            memory_layer.commitChanges()
            memory_layer.updateExtents()
            
            if features_added > 0:
                QgsProject.instance().addMapLayer(memory_layer)
                return True
            
            return False
        except:
            return False
    
    def _view_data(self):
        """View table data in a query dialog"""
        try:
            full_table_name = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
            
            dialog = DatabricksQueryDialog(
                self.connection_config,
                QgsApplication.instance().activeWindow(),
                initial_query=f"SELECT * FROM {full_table_name} LIMIT 100"
            )
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(
                QgsApplication.instance().activeWindow(),
                "Error",
                f"Failed to view data: {str(e)}"
            )


class DatabricksColumnItem(QgsDataItem):
    """Item representing a table column"""
    
    def __init__(self, parent, display_name, column_name, column_type, is_geometry):
        super().__init__(QgsDataItem.Field, parent, display_name, parent.path() + "/" + column_name)
        self.column_name = column_name
        self.column_type = column_type
        self.is_geometry = is_geometry
        
        # Set appropriate icon based on column type
        if is_geometry:
            self.setIcon(QgsApplication.getThemeIcon('/mIconGeometryEditVertexTool.svg'))
        elif 'INT' in column_type.upper() or 'BIGINT' in column_type.upper():
            self.setIcon(QgsApplication.getThemeIcon('/mIconFieldInteger.svg'))
        elif 'DOUBLE' in column_type.upper() or 'FLOAT' in column_type.upper() or 'DECIMAL' in column_type.upper():
            self.setIcon(QgsApplication.getThemeIcon('/mIconFieldFloat.svg'))
        elif 'DATE' in column_type.upper() or 'TIMESTAMP' in column_type.upper():
            self.setIcon(QgsApplication.getThemeIcon('/mIconFieldDate.svg'))
        elif 'BOOL' in column_type.upper():
            self.setIcon(QgsApplication.getThemeIcon('/mIconFieldBool.svg'))
        else:
            # String and other types
            self.setIcon(QgsApplication.getThemeIcon('/mIconFieldText.svg'))

class DatabricksQueryItem(QgsDataItem):
    """Item for executing custom queries"""
    
    def __init__(self, parent, name, connection_config):
        super().__init__(QgsDataItem.Collection, parent, name, parent.path() + "/" + name)
        self.connection_config = connection_config
        self.setIcon(QgsApplication.getThemeIcon('/mActionRunSql.svg'))
    
    def actions(self, parent):
        """Return context menu actions"""
        actions = []
        
        # Execute query action
        query_action = QAction("Execute Query...", parent)
        query_action.triggered.connect(self._execute_query)
        actions.append(query_action)
        
        return actions
    
    def _execute_query(self):
        """Open custom query dialog"""
        try:
            dialog = DatabricksQueryDialog(
                self.connection_config,
                QgsApplication.instance().activeWindow()
            )
            dialog.exec_()
        except Exception as e:
            QMessageBox.critical(
                QgsApplication.instance().activeWindow(),
                "Error",
                f"Failed to open query dialog: {str(e)}"
            )


class DatabricksRootItem(QgsDataCollectionItem):
    """Root Databricks item in browser"""
    
    def __init__(self):
        super().__init__(None, "Databricks", "/Databricks")
        
        # Set custom Databricks icon
        icon_path = os.path.join(os.path.dirname(__file__), 'icons', 'databricks.svg')
        if os.path.exists(icon_path):
            self.setIcon(QIcon(icon_path))
        else:
            self.setIcon(QgsApplication.getThemeIcon('/mIconDbSchema.svg'))
        
    def createChildren(self):
        """Create connection children from saved connections"""
        children = []
        
        try:
            settings = QSettings()
            settings.beginGroup("DatabricksConnector/Connections")
            connection_names = settings.childGroups()
            
            for conn_name in connection_names:
                settings.beginGroup(conn_name)
                connection_config = {
                    'hostname': settings.value("hostname", ""),
                    'http_path': settings.value("http_path", ""),
                    'access_token': settings.value("access_token", "")
                }
                settings.endGroup()
                
                if all(connection_config.values()):  # Only add if all required fields are present
                    conn_item = DatabricksConnectionItem(self, conn_name, connection_config)
                    children.append(conn_item)
            
            settings.endGroup()
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error loading Databricks connections: {str(e)}",
                "Databricks Browser",
                Qgis.Warning
            )
        
        if not children:
            # No connections configured
            help_item = QgsErrorItem(self, "No connections configured. Use the Databricks connector plugin to create connections.", 
                                   "/Databricks/help")
            children.append(help_item)
        
        return children
    
    def actions(self, parent):
        """Return context menu actions"""
        actions = []
        
        # Refresh action
        refresh_action = QAction("Refresh", parent)
        refresh_action.triggered.connect(self.refresh)
        actions.append(refresh_action)
        
        return actions


class DatabricksDataItemProvider(QgsDataItemProvider):
    """Data item provider for Databricks"""
    
    def name(self):
        return "Databricks"
    
    def capabilities(self):
        return QgsDataProvider.Database
    
    def createDataItem(self, path, parentItem):
        # Create root item when QGIS asks for top-level items (empty path, no parent)
        if not path and parentItem is None:
            return DatabricksRootItem()
        return None
