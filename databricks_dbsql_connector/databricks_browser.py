"""
Databricks Browser Provider for QGIS Browser Panel
"""
import os
from typing import List, Dict, Any, Optional
from qgis.PyQt.QtCore import QThread, pyqtSignal, QSettings, QDate, QTime, QDateTime, QVariant
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
            
            # Add all catalogs first (these come sorted alphabetically from the query)
            for catalog in catalogs:
                catalog_item = DatabricksCatalogItem(self, catalog, self.connection_config)
                children.append(catalog_item)
            
            # Add custom query option at the end (after all catalogs)
            # Using a special name prefix to ensure it sorts to the bottom
            query_item = DatabricksQueryItem(self, "⚡ Custom Query", self.connection_config)
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
                    SELECT DISTINCT catalog_name
                    FROM system.information_schema.catalogs
                    ORDER BY catalog_name
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
                # Use system.information_schema.schemata to get all accessible schemas
                # This is more reliable than querying columns table
                info_query = f"""
                    SELECT DISTINCT schema_name
                    FROM system.information_schema.schemata
                    WHERE catalog_name = '{self.catalog_name}'
                        AND schema_name IS NOT NULL 
                    ORDER BY schema_name
                """
                
                QgsMessageLog.logMessage(
                    f"Browser: Querying schemas for catalog '{self.catalog_name}' with: {info_query}",
                    "Databricks Browser",
                    Qgis.Info
                )
                
                cursor.execute(info_query)
                results = cursor.fetchall()
                schemas = [row[0] for row in results if row[0]]
                
                QgsMessageLog.logMessage(
                    f"Browser: Found {len(schemas)} schemas in catalog '{self.catalog_name}': {schemas}",
                    "Databricks Browser",
                    Qgis.Info
                )
            
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
        """Get list of tables in this schema with geometry information using system.information_schema"""
        try:
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            tables = {}  # Use dict to store table information
            
            with connection.cursor() as cursor:
                # First, get all accessible tables using system.information_schema.tables
                tables_query = f"""
                    SELECT DISTINCT table_name
                    FROM system.information_schema.tables
                    WHERE table_catalog = '{self.catalog_name}'
                        AND table_schema = '{self.schema_name}'
                        AND table_name IS NOT NULL
                    ORDER BY table_name
                """
                
                QgsMessageLog.logMessage(
                    f"Browser: Querying tables for {self.catalog_name}.{self.schema_name} with: {tables_query}",
                    "Databricks Browser",
                    Qgis.Info
                )
                
                cursor.execute(tables_query)
                table_results = cursor.fetchall()
                
                # Initialize all tables
                for row in table_results:
                    table_name = row[0]
                    tables[table_name] = {
                        'table_name': table_name,
                        'geometry_column': None,
                        'geometry_type': None,
                        'has_geometry': False
                    }
                
                QgsMessageLog.logMessage(
                    f"Browser: Found {len(tables)} tables in {self.catalog_name}.{self.schema_name}",
                    "Databricks Browser",
                    Qgis.Info
                )
                
                # Now check for geometry columns using system.information_schema.columns
                if tables:
                    columns_query = f"""
                        SELECT table_name, column_name, data_type
                        FROM system.information_schema.columns 
                        WHERE table_catalog = '{self.catalog_name}'
                            AND table_schema = '{self.schema_name}'
                            AND table_name IS NOT NULL 
                            AND data_type IN ('GEOMETRY', 'GEOGRAPHY')
                        ORDER BY table_name, column_name
                    """
                    
                    QgsMessageLog.logMessage(
                        f"Browser: Querying geometry columns with: {columns_query}",
                        "Databricks Browser",
                        Qgis.Info
                    )
                    
                    cursor.execute(columns_query)
                    column_results = cursor.fetchall()
                    
                    # Update tables with geometry information
                    for row in column_results:
                        table_name = row[0]
                        column_name = row[1]
                        data_type = row[2]
                        
                        if table_name in tables:
                            tables[table_name]['geometry_column'] = column_name
                            tables[table_name]['geometry_type'] = data_type
                            tables[table_name]['has_geometry'] = True
                    
                    geom_tables = sum(1 for t in tables.values() if t['has_geometry'])
                    QgsMessageLog.logMessage(
                        f"Browser: {geom_tables} out of {len(tables)} tables have geometry columns",
                        "Databricks Browser",
                        Qgis.Info
                    )
            
            connection.close()
            return list(tables.values())
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error getting tables from system.information_schema: {str(e)}",
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
    
    def _escape_identifier(self, identifier):
        """Escape identifier with backticks if it contains special characters"""
        if not identifier:
            return identifier
        # Always use backticks for safety, especially if identifier contains hyphens or special chars
        # Remove existing backticks first to avoid double-escaping
        identifier = identifier.strip('`')
        return f"`{identifier}`"
    
    def _get_table_reference(self):
        """Get properly escaped table reference in format catalog.schema.table"""
        catalog = self._escape_identifier(self.catalog_name)
        schema = self._escape_identifier(self.schema_name)
        table = self._escape_identifier(self.table_name)
        return f"{catalog}.{schema}.{table}"
    
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
                table_ref = self._get_table_reference()
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
            # Add first 1000 features action
            add_1000_action = QAction("Add First 1000 Features", parent)
            add_1000_action.triggered.connect(lambda: self._add_layer(max_features=1000))
            actions.append(add_1000_action)
            
            # Set as default action for double-click
            add_1000_action.setData("default")
            
            # Add all features action
            add_all_action = QAction("Add All Features", parent)
            add_all_action.triggered.connect(lambda: self._add_layer(max_features=0))
            actions.append(add_all_action)
        
        # View data action (for both geometry and non-geometry tables)
        view_action = QAction("View Data...", parent)
        view_action.triggered.connect(self._view_data)
        actions.append(view_action)
        
        return actions
    
    def handleDoubleClick(self):
        """Handle double-click on table item"""
        if self.table_info['has_geometry']:
            self._add_layer(max_features=1000)  # Default to 1000 on double-click
            return True
        return False
    
    def _add_layer(self, max_features=1000):
        """Add this table as a layer to QGIS using simplified synchronous approach
        
        Args:
            max_features: Maximum number of features to load. 0 means unlimited.
        """
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
            
            # Get layer prefix from settings (same setting used by dialog)
            settings = QSettings()
            layer_prefix = settings.value("DatabricksConnector/LayerPrefix", "databricks_")
            
            # Build layer name with prefix
            layer_name = f"{layer_prefix}{self.table_name}"
            
            limit_msg = f"first {max_features} features" if max_features > 0 else "all features"
            QgsMessageLog.logMessage(
                f"Loading table: {layer_name} ({limit_msg})",
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
                table_ref = self._get_table_reference()
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
                        # Map databricks types to QGIS types (consistent with dialog)
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
                geometry_sql = f"ST_ASWKT({self._escape_identifier(geometry_column)})" if geometry_column else "NULL"
                
                # Get attribute columns (excluding geometry) - escape them too
                attr_columns = [self._escape_identifier(f.name()) for f in fields]
                attr_sql = ", ".join(attr_columns) if attr_columns else "1"
                
                # Build query with optional LIMIT clause
                data_query = f"""
                    SELECT {attr_sql}, {geometry_sql} as geometry_wkt
                    FROM {table_ref}
                """
                
                # Add LIMIT clause only if max_features > 0
                if max_features > 0:
                    data_query += f"\n                    LIMIT {max_features}"
                
                QgsMessageLog.logMessage(
                    f"Executing query: {data_query}",
                    "Databricks Browser",
                    Qgis.Info
                )
                
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
                if self._create_geometry_layer(layer_name, geom_type, type_rows, fields, max_features):
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
    
    def _create_geometry_layer(self, base_layer_name, geom_type, rows, fields, max_features=1000):
        """Create a memory layer for a specific geometry type.
        
        Uses direct provider access (no edit mode) to avoid strict type validation
        issues with NULL/empty datetime values.
        Uses Multi* geometry types to handle both single and multi-part geometries.
        """
        try:
            from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsWkbTypes, QgsProject
            
            # Use Multi* geometry types to accept both single and multi-part geometries
            multi_geom_map = {
                'Point': 'MultiPoint',
                'LineString': 'MultiLineString',
                'Polygon': 'MultiPolygon'
            }
            qgis_geom_type = multi_geom_map.get(geom_type, geom_type)
            
            layer_name = f"{base_layer_name}_{geom_type}"
            memory_layer = QgsVectorLayer(f"{qgis_geom_type}?crs=EPSG:4326", layer_name, "memory")
            
            if not memory_layer.isValid():
                return False
            
            # Add fields directly to provider (no edit mode needed)
            provider = memory_layer.dataProvider()
            provider.addAttributes(fields.toList())
            memory_layer.updateFields()
            
            # Build features list
            features_to_add = []
            layer_fields = memory_layer.fields()
            
            for row in rows:
                try:
                    feature = QgsFeature(layer_fields)
                    raw_attrs = list(row[:-1]) if len(row) > 1 else []
                    
                    # Process attributes with proper type conversion (consistent with dialog)
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
                                # Convert Python datetime to QDateTime (same as dialog)
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
                    
                    wkt_geom = row[-1] if row else None
                    if wkt_geom and wkt_geom != 'NULL':
                        clean_wkt = self._strip_srid_from_wkt(str(wkt_geom))
                        geom = QgsGeometry.fromWkt(clean_wkt)
                        
                        if not geom.isEmpty():
                            # Convert to multi-part for compatibility with Multi* layer
                            if not geom.isMultipart():
                                geom.convertToMultiType()
                            feature.setGeometry(geom)
                            features_to_add.append(feature)
                except Exception as e:
                    # Log errors instead of silently passing
                    QgsMessageLog.logMessage(
                        f"Error processing feature: {str(e)}",
                        "Databricks Browser",
                        Qgis.Warning
                    )
            
            # Add features directly to provider (bypasses edit buffer type validation)
            if features_to_add:
                success, added_features = provider.addFeatures(features_to_add)
                QgsMessageLog.logMessage(
                    f"addFeatures returned: success={success}, added count={len(added_features) if added_features else 0}",
                    "Databricks Browser",
                    Qgis.Info
                )
            
            memory_layer.updateExtents()
            
            # Use featureCount() to check actual features - addFeatures can return False even if some succeed
            final_count = memory_layer.featureCount()
            
            QgsMessageLog.logMessage(
                f"Layer created: {layer_name}, final feature count: {final_count}",
                "Databricks Browser",
                Qgis.Info
            )
            
            if final_count > 0:
                # Store Databricks metadata for refresh functionality
                self._store_layer_metadata(memory_layer, max_features)
                
                QgsProject.instance().addMapLayer(memory_layer)
                return True
            
            return False
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error creating geometry layer: {str(e)}",
                "Databricks Browser",
                Qgis.Critical
            )
            return False
    
    def _store_layer_metadata(self, layer, max_features=1000):
        """Store Databricks connection and table metadata on the layer for refresh functionality"""
        try:
            # Store connection config
            layer.setCustomProperty("databricks/hostname", self.connection_config.get('hostname', ''))
            layer.setCustomProperty("databricks/http_path", self.connection_config.get('http_path', ''))
            layer.setCustomProperty("databricks/access_token", self.connection_config.get('access_token', ''))
            
            # Store table info
            full_name = f"{self.catalog_name}.{self.schema_name}.{self.table_name}"
            layer.setCustomProperty("databricks/full_name", full_name)
            layer.setCustomProperty("databricks/geometry_column", self.table_info.get('geometry_column', ''))
            layer.setCustomProperty("databricks/geometry_type", self.table_info.get('geometry_type', ''))
            layer.setCustomProperty("databricks/max_features", str(max_features))
            layer.setCustomProperty("databricks/is_databricks_layer", "true")
            
            QgsMessageLog.logMessage(
                f"Stored Databricks metadata on layer: {layer.name()}",
                "Databricks Browser",
                Qgis.Info
            )
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error storing layer metadata: {str(e)}",
                "Databricks Browser",
                Qgis.Warning
            )
    
    def _view_data(self):
        """View table data in a query dialog"""
        try:
            full_table_name = self._get_table_reference()
            
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
    
    def sortKey(self):
        """Return sort key to control position in browser panel.
        
        Returns 'aaa' prefix to sort near the top alphabetically.
        """
        return "aaa_Databricks"
        
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
