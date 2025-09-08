"""
Fixed dialog with proper geometry handling and connection settings persistence
"""
import os
import json
from qgis.PyQt.QtCore import Qt, QThread, pyqtSignal, QSettings
from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QComboBox, 
    QTableWidget, QTableWidgetItem, QMessageBox,
    QProgressDialog, QHeaderView, QCheckBox,
    QGroupBox, QTextEdit, QPlainTextEdit, QSplitter,
    QTreeWidget, QTreeWidgetItem, QWidget
)
from qgis.core import (
    QgsVectorLayer, QgsProject, QgsDataSourceUri,
    QgsMessageLog, Qgis, QgsFeature, QgsFields,
    QgsField, QgsGeometry, QgsWkbTypes, QgsMemoryProviderUtils,
    QgsCoordinateReferenceSystem
)
from qgis.PyQt.QtCore import QVariant, QDateTime, QDate, QTime

# Check if databricks is available
try:
    from databricks import sql
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False

try:
    from shapely import wkt
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False

# Query dialog classes will be defined in this file to avoid import issues
QUERY_DIALOG_AVAILABLE = True
QUERY_DIALOG_IMPORT_ERROR = None


class ConnectionTestThread(QThread):
    """Thread for testing Databricks connection"""
    
    finished = pyqtSignal(bool, str)  # success, message
    
    def __init__(self, hostname, http_path, access_token):
        super().__init__()
        self.hostname = hostname
        self.http_path = http_path
        self.access_token = access_token
    
    def run(self):
        if not DATABRICKS_AVAILABLE:
            self.finished.emit(False, "databricks-sql-connector not installed")
            return
            
        try:
            connection = sql.connect(
                server_hostname=self.hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            
            # Test with a simple query
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            
            connection.close()
            self.finished.emit(True, "Connection successful!")
            
        except Exception as e:
            self.finished.emit(False, f"Connection failed: {str(e)}")


class TableDiscoveryThread(QThread):
    """Thread for discovering spatial tables"""
    
    finished = pyqtSignal(list)  # list of table info dicts
    
    def __init__(self, hostname, http_path, access_token):
        super().__init__()
        self.hostname = hostname
        self.http_path = http_path
        self.access_token = access_token
    
    def run(self):
        tables = []
        if not DATABRICKS_AVAILABLE:
            self.finished.emit(tables)
            return
            
        try:
            connection = sql.connect(
                server_hostname=self.hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            
            with connection.cursor() as cursor:
                # Query to find tables with spatial columns
                query = """
                SELECT 
                    table_catalog,
                    table_schema,
                    table_name,
                    column_name,
                    data_type
                FROM information_schema.columns 
                WHERE data_type IN ('GEOGRAPHY', 'GEOMETRY')
                ORDER BY table_catalog, table_schema, table_name
                """
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                for row in results:
                    tables.append({
                        'catalog': row[0] or '',
                        'schema': row[1] or '',
                        'table': row[2],
                        'geometry_column': row[3],
                        'geometry_type': row[4],
                        'full_name': f"{row[0]}.{row[1]}.{row[2]}" if row[0] and row[1] else (f"{row[1]}.{row[2]}" if row[1] else row[2])
                    })
            
            connection.close()
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error discovering tables: {str(e)}",
                "Databricks Connector",
                Qgis.Critical
            )
        
        self.finished.emit(tables)


class LayerLoadingThread(QThread):
    """Thread for loading data from Databricks into QGIS memory layers - FIXED VERSION"""
    
    progress = pyqtSignal(str)  # progress message
    finished = pyqtSignal(bool, str, object)  # success, message, layer_object
    
    def __init__(self, hostname, http_path, access_token, table_info, layer_name, max_features=1000):
        super().__init__()
        self.hostname = hostname
        self.http_path = http_path
        self.access_token = access_token
        self.table_info = table_info
        self.layer_name = layer_name
        self.max_features = max_features
    
    def run(self):
        if not DATABRICKS_AVAILABLE:
            self.finished.emit(False, "databricks-sql-connector not installed", None)
            return
        
        try:
            self.progress.emit("Connecting to Databricks...")
            
            connection = sql.connect(
                server_hostname=self.hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            
            # If geometry type is generic, detect actual type from sample data
            if self.table_info['geometry_type'].upper().startswith('GEOMETRY'):
                self._detect_mixed_geometry_types(connection)
            
            with connection.cursor() as cursor:
                self.progress.emit("Querying table schema...")
                
                # Get table schema - EXCLUDE geometry column from attributes
                table_ref = self.table_info['full_name']
                cursor.execute(f"DESCRIBE {table_ref}")
                schema_info = cursor.fetchall()
                
                # Build QGIS fields - EXCLUDE geometry column
                fields = QgsFields()
                geometry_column = self.table_info['geometry_column']
                
                QgsMessageLog.logMessage(
                    f"Processing schema for table {table_ref}, geometry column: {geometry_column}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                for row in schema_info:
                    col_name = row[0]
                    col_type = row[1].upper()

                    QgsMessageLog.logMessage(
                        f"Column: {col_name}, Type: {col_type}",
                        "Databricks Connector",
                        Qgis.Info
                    )

                    # CRITICAL FIX: Skip geometry column from attributes
                    # Check both by name and by type (for robustness)
                    if col_name.lower() != geometry_column.lower() and not col_type.startswith(('GEOGRAPHY', 'GEOMETRY')):
                        # ADDITIONAL CHECK: Skip columns that contain WKT geometry data
                        # This is a heuristic - if column name suggests it contains geometry, skip it
                        if col_name.lower() in ['location', 'geom', 'wkt', 'geometry_text', 'point', 'polygon', 'linestring']:
                            QgsMessageLog.logMessage(
                                f"Skipping potential geometry text column: {col_name} ({col_type})",
                                "Databricks Connector",
                                Qgis.Info
                            )
                        else:
                            qgs_type = self._map_databricks_type_to_qgs(col_type)
                            field = QgsField(col_name, qgs_type)
                            fields.append(field)
                            QgsMessageLog.logMessage(
                                f"Added attribute field: {col_name} ({qgs_type})",
                                "Databricks Connector",
                                Qgis.Info
                            )
                    else:
                        # This is the geometry column - skip it from attributes
                        QgsMessageLog.logMessage(
                            f"Skipping geometry column: {col_name} ({col_type})",
                            "Databricks Connector",
                            Qgis.Info
                        )
                
                self.progress.emit("Fetching data...")
                
                # Query data - Get attributes AND geometry separately
                # CRITICAL: Build query to match exactly the fields we added to the layer
                attribute_fields = [f.name() for f in fields]

                # Build query with geometry as WKT - only select fields that are in the layer
                select_clause = attribute_fields.copy()
                select_clause.append(f"ST_ASWKT({geometry_column}) as geom_wkt")

                query = f"SELECT {', '.join(select_clause)} FROM {table_ref}"

                if self.max_features > 0:
                    query += f" LIMIT {self.max_features}"
                
                QgsMessageLog.logMessage(
                    f"Query fields: {select_clause}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                QgsMessageLog.logMessage(
                    f"Executing query: {query}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                QgsMessageLog.logMessage(
                    f"Retrieved {len(rows)} rows",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                self.progress.emit("Creating QGIS layer...")
                
                # Determine geometry type for layer creation
                geom_type = self._get_qgs_geometry_type()
                wkb_geom_type = self._get_wkb_geometry_type()

                # Create memory layer with proper geometry type
                layer_def = f"{geom_type}?crs=EPSG:4326"
                memory_layer = QgsVectorLayer(layer_def, self.layer_name, "memory")

                if not memory_layer.isValid():
                    self.finished.emit(False, f"Failed to create memory layer: {layer_def}", None)
                    return

                # Log memory layer details for debugging
                QgsMessageLog.logMessage(
                    f"Memory layer created: {layer_def}, WKB type: {memory_layer.wkbType()}, "
                    f"detected WKB: {wkb_geom_type}, provider valid: {memory_layer.dataProvider().isValid()}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                QgsMessageLog.logMessage(
                    f"Created memory layer: {layer_def}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                # Start editing to add fields and features
                edit_started = memory_layer.startEditing()
                QgsMessageLog.logMessage(
                    f"Started editing mode: {edit_started}, layer editable: {memory_layer.isEditable()}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                # Add fields to layer (NO geometry field)
                provider = memory_layer.dataProvider()
                add_result = provider.addAttributes(fields.toList())
                if not add_result:
                    QgsMessageLog.logMessage(
                        f"Failed to add attributes to layer provider",
                        "Databricks Connector",
                        Qgis.Critical
                    )
                
                memory_layer.updateFields()
                
                QgsMessageLog.logMessage(
                    f"Added {len(fields)} attribute fields to layer. Add result: {add_result}, "
                    f"layer field count: {memory_layer.fields().count()}, "
                    f"provider capabilities: {provider.capabilities()}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                self.progress.emit(f"Loading {len(rows)} features...")
                
                # Add features
                features_to_add = []
                valid_features = 0
                
                # CRITICAL FIX: Use the layer's fields after they've been added and updated
                layer_fields = memory_layer.fields()
                
                for i, row in enumerate(rows):
                    try:
                        # Create feature with incremental ID
                        feature = QgsFeature(layer_fields, i + 1)
                        
                        # Set attributes - should now match fields list exactly (no geometry column)
                        attrs = list(row[:-1])  # All except last (geometry WKT)

                        # Debug: Log the raw attribute data
                        QgsMessageLog.logMessage(
                            f"Feature {i} raw attributes: {attrs}",
                            "Databricks Connector",
                            Qgis.Info
                        )
                        
                        # Debug: Log attribute types
                        attr_types = [type(attr).__name__ for attr in attrs]
                        QgsMessageLog.logMessage(
                            f"Feature {i} attribute types: {attr_types}",
                            "Databricks Connector",
                            Qgis.Info
                        )

                        # Since query now matches layer fields exactly, attributes should align
                        layer_field_names = [f.name() for f in layer_fields]
                        
                        # Process attributes with proper type conversion
                        processed_attrs = []
                        for j, attr_value in enumerate(attrs):
                            if j < len(layer_fields):
                                field = layer_fields[j]
                                field_type = field.type()
                                
                                # Convert attribute to proper type
                                if attr_value is None:
                                    processed_attrs.append(None)
                                elif field_type == QVariant.LongLong:
                                    processed_attrs.append(int(attr_value) if attr_value is not None else None)
                                elif field_type == QVariant.String:
                                    processed_attrs.append(str(attr_value) if attr_value is not None else None)
                                elif field_type == QVariant.DateTime:
                                    # CRITICAL FIX: Convert datetime to QDateTime for QGIS compatibility
                                    if attr_value is not None:
                                        if hasattr(attr_value, 'year'):  # It's a datetime object
                                            # Convert Python datetime to QDateTime
                                            qdate = QDate(attr_value.year, attr_value.month, attr_value.day)
                                            qtime = QTime(attr_value.hour, attr_value.minute, attr_value.second, attr_value.microsecond // 1000)
                                            qdt = QDateTime(qdate, qtime)
                                            processed_attrs.append(qdt)
                                        else:
                                            processed_attrs.append(attr_value)  # Already in correct format
                                    else:
                                        processed_attrs.append(None)
                                else:
                                    processed_attrs.append(attr_value)
                            else:
                                break

                        # Verify attribute count matches field count
                        if len(processed_attrs) != len(layer_fields):
                            QgsMessageLog.logMessage(
                                f"Attribute count mismatch - expected {len(layer_fields)}, got {len(processed_attrs)}. "
                                f"Layer fields: {layer_field_names}",
                                "Databricks Connector",
                                Qgis.Warning
                            )

                        # Debug: Log processed attributes
                        processed_attr_types = [type(attr).__name__ for attr in processed_attrs]
                        QgsMessageLog.logMessage(
                            f"Feature {i} processed attributes: {processed_attrs}",
                            "Databricks Connector",
                            Qgis.Info
                        )
                        QgsMessageLog.logMessage(
                            f"Feature {i} processed attribute types: {processed_attr_types}",
                            "Databricks Connector",
                            Qgis.Info
                        )
                        
                        feature.setAttributes(processed_attrs)
                        
                        # CRITICAL FIX: Set geometry separately from WKT
                        geom_wkt = row[-1]  # Last column is geometry WKT
                        if geom_wkt and geom_wkt.strip():
                            try:
                                # Parse geometry using QGIS built-in WKT parser
                                geometry = QgsGeometry.fromWkt(geom_wkt)
                                
                                if geometry.isNull() or not geometry.isGeosValid():
                                    QgsMessageLog.logMessage(
                                        f"Invalid geometry for feature {i}: {geom_wkt[:100]}...",
                                        "Databricks Connector",
                                        Qgis.Warning
                                    )
                                    continue
                                
                                # Check geometry compatibility with layer
                                feature_wkb = geometry.wkbType()
                                layer_wkb = memory_layer.wkbType()
                                
                                # Handle geometry filtering based on layer type
                                target_geom_type = self.table_info.get('target_geometry_type')
                                
                                if target_geom_type:
                                    # This is a specific geometry type layer (LineString or Polygon)
                                    expected_wkb = 2 if target_geom_type == 'ST_LINESTRING' else 3  # LineString or Polygon
                                    if feature_wkb != expected_wkb:
                                        QgsMessageLog.logMessage(
                                            f"Skipping geometry type {feature_wkb} (expected {expected_wkb}) for feature {i}: {row[1]}",
                                            "Databricks Connector",
                                            Qgis.Info
                                        )
                                        continue
                                elif self.table_info.get('mixed_geometries', False):
                                    # For mixed geometries, only add Points to Point layer
                                    if feature_wkb != 1:  # Not a Point
                                        QgsMessageLog.logMessage(
                                            f"Skipping non-Point geometry (type {feature_wkb}) in Point layer for feature {i}: {row[1]}",
                                            "Databricks Connector",
                                            Qgis.Info
                                        )
                                        continue
                                elif feature_wkb != layer_wkb:
                                    QgsMessageLog.logMessage(
                                        f"Geometry type mismatch - Feature: {feature_wkb}, Layer: {layer_wkb}. "
                                        f"Skipping feature {i}.",
                                        "Databricks Connector",
                                        Qgis.Warning
                                    )
                                    continue  # Skip incompatible features
                                
                                feature.setGeometry(geometry)
                                
                                # Validate the complete feature before adding
                                if feature.isValid() and not feature.geometry().isNull():
                                    features_to_add.append(feature)
                                    valid_features += 1
                                    
                                    QgsMessageLog.logMessage(
                                        f"Feature {i} created successfully - ID: {feature.id()}, "
                                        f"Attrs: {len(feature.attributes())}, Geom: {not feature.geometry().isNull()}",
                                        "Databricks Connector",
                                        Qgis.Info
                                    )
                                else:
                                    QgsMessageLog.logMessage(
                                        f"Feature {i} validation failed",
                                        "Databricks Connector",
                                        Qgis.Warning
                                    )
                                
                            except Exception as geom_e:
                                QgsMessageLog.logMessage(
                                    f"Error parsing geometry for feature {i}: {str(geom_e)}, WKT: {geom_wkt[:100]}",
                                    "Databricks Connector",
                                    Qgis.Warning
                                )
                                continue
                        else:
                            QgsMessageLog.logMessage(
                                f"Empty geometry for feature {i}",
                                "Databricks Connector",
                                Qgis.Warning
                            )
                            
                    except Exception as feat_e:
                        QgsMessageLog.logMessage(
                            f"Error processing feature {i}: {str(feat_e)}",
                            "Databricks Connector",
                            Qgis.Critical
                        )
                        continue
                
                # Add features to layer
                if features_to_add:
                    QgsMessageLog.logMessage(
                        f"Attempting to add {len(features_to_add)} features to layer",
                        "Databricks Connector",
                        Qgis.Info
                    )

                    # Debug: Log layer and feature field information
                    layer_field_names = [f.name() for f in memory_layer.fields()]
                    layer_field_types = [f.type() for f in memory_layer.fields()]
                    QgsMessageLog.logMessage(
                        f"Layer fields: {layer_field_names}",
                        "Databricks Connector",
                        Qgis.Info
                    )
                    QgsMessageLog.logMessage(
                        f"Layer field types: {layer_field_types}",
                        "Databricks Connector",
                        Qgis.Info
                    )

                    if features_to_add:
                        first_feature = features_to_add[0]
                        first_attr_types = [type(attr) for attr in first_feature.attributes()]
                        QgsMessageLog.logMessage(
                            f"First feature attributes count: {len(first_feature.attributes())}, "
                            f"geometry valid: {not first_feature.geometry().isNull()}, "
                            f"geometry type: {first_feature.geometry().wkbType()}, "
                            f"attribute types: {first_attr_types}",
                            "Databricks Connector",
                            Qgis.Info
                        )

                        # Check if feature is valid for the layer
                        is_valid = first_feature.isValid()
                        QgsMessageLog.logMessage(
                            f"First feature is valid: {is_valid}",
                            "Databricks Connector",
                            Qgis.Info
                        )

                    # Try different approaches to add features
                    successful_adds = 0
                    
                    # Method 1: Try using layer.addFeature() instead of dataProvider().addFeatures()
                    QgsMessageLog.logMessage(
                        "Trying Method 1: layer.addFeature()",
                        "Databricks Connector",
                        Qgis.Info
                    )
                    
                    for i, feature in enumerate(features_to_add):
                        try:
                            # Use the layer's addFeature method instead of dataProvider
                            add_success = memory_layer.addFeature(feature)
                            if add_success:
                                successful_adds += 1
                                QgsMessageLog.logMessage(
                                    f"Successfully added feature {i} using layer.addFeature()",
                                    "Databricks Connector",
                                    Qgis.Info
                                )
                            else:
                                QgsMessageLog.logMessage(
                                    f"Failed to add feature {i} using layer.addFeature()",
                                    "Databricks Connector",
                                    Qgis.Warning
                                )
                                
                                # Method 2: Try with dataProvider if layer method fails
                                QgsMessageLog.logMessage(
                                    f"Trying Method 2 for feature {i}: dataProvider.addFeatures()",
                                    "Databricks Connector",
                                    Qgis.Info
                                )
                                
                                single_result = memory_layer.dataProvider().addFeatures([feature])
                                if single_result[0]:
                                    successful_adds += 1
                                    QgsMessageLog.logMessage(
                                        f"Successfully added feature {i} using dataProvider",
                                        "Databricks Connector",
                                        Qgis.Info
                                    )
                                else:
                                    QgsMessageLog.logMessage(
                                        f"Both methods failed for feature {i}. DataProvider result: {single_result}",
                                        "Databricks Connector",
                                        Qgis.Critical
                                    )
                                    break
                        except Exception as e:
                            QgsMessageLog.logMessage(
                                f"Exception adding feature {i}: {str(e)}",
                                "Databricks Connector",
                                Qgis.Critical
                            )
                            break

                    QgsMessageLog.logMessage(
                        f"Total successful feature additions: {successful_adds} out of {len(features_to_add)}",
                        "Databricks Connector",
                        Qgis.Info
                    )
                else:
                    QgsMessageLog.logMessage(
                        "No valid features to add to layer",
                        "Databricks Connector",
                        Qgis.Warning
                    )
                
                # Commit changes and update extents
                if memory_layer.isEditable():
                    commit_result = memory_layer.commitChanges()
                    QgsMessageLog.logMessage(
                        f"Commit changes result: {commit_result}, layer feature count: {memory_layer.featureCount()}",
                        "Databricks Connector",
                        Qgis.Info
                    )
                    
                    if not commit_result:
                        # Try to get commit errors
                        errors = memory_layer.commitErrors()
                        QgsMessageLog.logMessage(
                            f"Commit errors: {errors}",
                            "Databricks Connector",
                            Qgis.Critical
                        )
                else:
                    QgsMessageLog.logMessage(
                        f"Layer not in editing mode, feature count: {memory_layer.featureCount()}",
                        "Databricks Connector",
                        Qgis.Info
                    )
                
                memory_layer.updateExtents()
                
                QgsMessageLog.logMessage(
                    f"Layer extent: {memory_layer.extent().toString()}, "
                    f"final feature count: {memory_layer.featureCount()}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                if memory_layer.featureCount() == 0:
                    self.finished.emit(False, "No features were successfully added to the layer", None)
                    return
                
            connection.close()
            
            # If we have mixed geometries, we need to create additional layers for LineStrings and Polygons
            if self.table_info.get('mixed_geometries', False):
                self.finished.emit(True, f"Loaded {memory_layer.featureCount()} Point features. Creating additional layers for LineStrings and Polygons...", memory_layer)
            else:
                self.finished.emit(True, f"Loaded {memory_layer.featureCount()} features with geometries", memory_layer)
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error in LayerLoadingThread: {str(e)}",
                "Databricks Connector",
                Qgis.Critical
            )
            self.finished.emit(False, f"Error loading layer: {str(e)}", None)
    
    def _map_databricks_type_to_qgs(self, databricks_type: str) -> QVariant.Type:
        """Map Databricks data types to QVariant types"""
        type_mapping = {
            'STRING': QVariant.String,
            'INT': QVariant.Int,
            'BIGINT': QVariant.LongLong,
            'FLOAT': QVariant.Double,
            'DOUBLE': QVariant.Double,
            'DECIMAL': QVariant.Double,
            'BOOLEAN': QVariant.Bool,
            'DATE': QVariant.Date,
            'TIMESTAMP': QVariant.DateTime,
            'TIMESTAMP_NTZ': QVariant.DateTime,
        }
        return type_mapping.get(databricks_type.upper(), QVariant.String)
    
    def _detect_mixed_geometry_types(self, connection):
        """Detect if table contains mixed geometry types and handle accordingly"""
        try:
            with connection.cursor() as cursor:
                table_ref = self.table_info['full_name']
                geometry_column = self.table_info['geometry_column']
                
                # Query to detect all geometry types in the table
                query = f"""
                SELECT DISTINCT ST_GEOMETRYTYPE({geometry_column}) as geom_type 
                FROM {table_ref} 
                WHERE {geometry_column} IS NOT NULL 
                LIMIT 10
                """
                
                QgsMessageLog.logMessage(
                    f"Detecting geometry types with query: {query}",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                cursor.execute(query)
                results = cursor.fetchall()
                
                if results:
                    geometry_types = [row[0].upper() for row in results]
                    QgsMessageLog.logMessage(
                        f"Found geometry types: {geometry_types}",
                        "Databricks Connector",
                        Qgis.Info
                    )
                    
                    # Check if we have mixed geometry types
                    if len(geometry_types) > 1:
                        # Mixed geometry types - store all types for separate layer creation
                        self.table_info['geometry_type'] = 'MIXED'
                        self.table_info['mixed_geometries'] = True
                        self.table_info['geometry_types_list'] = geometry_types
                        QgsMessageLog.logMessage(
                            f"Mixed geometry types detected: {geometry_types}. Will create separate layers for each type.",
                            "Databricks Connector",
                            Qgis.Info
                        )
                    else:
                        # Single geometry type
                        detected_type = geometry_types[0]
                        self.table_info['geometry_type'] = detected_type
                        self.table_info['mixed_geometries'] = False
                        QgsMessageLog.logMessage(
                            f"Single geometry type detected: {detected_type}",
                            "Databricks Connector",
                            Qgis.Info
                        )
                else:
                    # No geometries found, default to Point
                    self.table_info['geometry_type'] = 'POINT'
                    self.table_info['mixed_geometries'] = False
                
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error detecting geometry types: {str(e)}. Using Point as default.",
                "Databricks Connector",
                Qgis.Warning
            )
            self.table_info['geometry_type'] = 'POINT'
            self.table_info['mixed_geometries'] = False
    
    def _get_qgs_geometry_type(self):
        """Get QGIS geometry type string for memory layer"""
        geom_type = self.table_info['geometry_type'].upper()
        
        # For mixed geometries, default to Point for now (we'll create separate layers later)
        if self.table_info.get('mixed_geometries', False):
            return "Point"  # Start with Point layer for mixed geometries
        
        # Handle specific geometry types
        if geom_type.startswith('GEOMETRY') or geom_type == 'MIXED':
            return "Point"  # Default to Point for generic types
        elif 'POINT' in geom_type:
            return "Point"
        elif 'LINESTRING' in geom_type:
            return "LineString"
        elif 'POLYGON' in geom_type:
            return "Polygon"
        else:
            return "Point"  # Default to Point for unknown types

    def _get_wkb_geometry_type(self):
        """Get WKB geometry type constant for comparison"""
        geom_type = self.table_info['geometry_type'].upper()
        
        # For mixed geometries, use Point WKB type for now
        if self.table_info.get('mixed_geometries', False):
            return 1  # Point WKB type
        
        # Handle specific geometry types
        if geom_type.startswith('GEOMETRY') or geom_type == 'MIXED':
            return 1  # Point for generic types
        elif 'POINT' in geom_type:
            return 1  # Point
        elif 'LINESTRING' in geom_type:
            return 2  # LineString
        elif 'POLYGON' in geom_type:
            return 3  # Polygon
        else:
            return 1  # Point for unknown types


class DatabricksDialog(QDialog):
    """Main dialog for Databricks connector with connection persistence"""
    
    def __init__(self, iface):
        super().__init__()
        self.iface = iface
        self.tables = []
        self.settings = QSettings()
        
        self.setup_ui()
        self.load_saved_connections()
        self.check_dependencies()
    
    def setup_ui(self):
        """Setup the user interface"""
        self.setWindowTitle("Connect to Databricks SQL")
        self.setModal(True)
        self.resize(800, 600)
        
        # Main layout
        layout = QVBoxLayout(self)
        
        # Connection settings group
        conn_group = QGroupBox("Connection Settings")
        conn_layout = QGridLayout(conn_group)
        
        # Saved connections dropdown
        conn_layout.addWidget(QLabel("Saved Connections:"), 0, 0)
        self.saved_connections_combo = QComboBox()
        self.saved_connections_combo.addItem("New Connection...")
        self.saved_connections_combo.currentTextChanged.connect(self.load_selected_connection)
        conn_layout.addWidget(self.saved_connections_combo, 0, 1)
        
        # Connection fields
        conn_layout.addWidget(QLabel("Connection Name:"), 1, 0)
        self.connection_name_edit = QLineEdit()
        conn_layout.addWidget(self.connection_name_edit, 1, 1)
        
        conn_layout.addWidget(QLabel("Server Hostname:"), 2, 0)
        self.hostname_edit = QLineEdit()
        self.hostname_edit.setPlaceholderText("your-workspace.cloud.databricks.com")
        conn_layout.addWidget(self.hostname_edit, 2, 1)
        
        conn_layout.addWidget(QLabel("HTTP Path:"), 3, 0)
        self.http_path_edit = QLineEdit()
        self.http_path_edit.setPlaceholderText("/sql/1.0/warehouses/your-warehouse-id")
        conn_layout.addWidget(self.http_path_edit, 3, 1)
        
        conn_layout.addWidget(QLabel("Access Token:"), 4, 0)
        self.access_token_edit = QLineEdit()
        self.access_token_edit.setEchoMode(QLineEdit.Password)
        self.access_token_edit.setPlaceholderText("dapi... (personal access token)")
        conn_layout.addWidget(self.access_token_edit, 4, 1)
        
        # Connection management buttons
        conn_mgmt_layout = QHBoxLayout()
        self.save_connection_btn = QPushButton("Save Connection")
        self.save_connection_btn.clicked.connect(self.save_current_connection)
        conn_mgmt_layout.addWidget(self.save_connection_btn)
        
        self.delete_connection_btn = QPushButton("Delete Connection")
        self.delete_connection_btn.clicked.connect(self.delete_saved_connection)
        conn_mgmt_layout.addWidget(self.delete_connection_btn)
        
        conn_mgmt_layout.addStretch()
        conn_layout.addLayout(conn_mgmt_layout, 5, 0, 1, 2)
        
        # Connection test buttons
        conn_btn_layout = QHBoxLayout()
        self.test_connection_btn = QPushButton("Test Connection")
        self.test_connection_btn.clicked.connect(self.test_connection)
        conn_btn_layout.addWidget(self.test_connection_btn)
        
        self.discover_tables_btn = QPushButton("Discover Tables")
        self.discover_tables_btn.clicked.connect(self.discover_tables)
        conn_btn_layout.addWidget(self.discover_tables_btn)
        
        conn_btn_layout.addStretch()
        conn_layout.addLayout(conn_btn_layout, 6, 0, 1, 2)
        
        layout.addWidget(conn_group)
        
        # Tables group
        tables_group = QGroupBox("Available Spatial Tables")
        tables_layout = QVBoxLayout(tables_group)
        
        # Table widget
        self.tables_widget = QTableWidget()
        self.tables_widget.setColumnCount(6)
        self.tables_widget.setHorizontalHeaderLabels([
            "Load", "Catalog", "Schema", "Table", "Geometry Column", "Geometry Type"
        ])
        self.tables_widget.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        tables_layout.addWidget(self.tables_widget)
        
        layout.addWidget(tables_group)
        
        # Layer options group
        options_group = QGroupBox("Layer Options")
        options_layout = QGridLayout(options_group)
        
        options_layout.addWidget(QLabel("Layer Name Prefix:"), 0, 0)
        self.layer_prefix_edit = QLineEdit()
        self.layer_prefix_edit.setText("databricks_")
        options_layout.addWidget(self.layer_prefix_edit, 0, 1)
        
        options_layout.addWidget(QLabel("Max Features:"), 1, 0)
        self.max_features_edit = QLineEdit()
        self.max_features_edit.setText("1000")
        options_layout.addWidget(self.max_features_edit, 1, 1)
        
        layout.addWidget(options_group)
        
        # Button box
        button_layout = QHBoxLayout()
        
        self.custom_query_btn = QPushButton("Custom Query...")
        self.custom_query_btn.clicked.connect(self.open_custom_query)
        button_layout.addWidget(self.custom_query_btn)
        
        self.add_layers_btn = QPushButton("Add Selected Layers")
        self.add_layers_btn.clicked.connect(self.add_selected_layers)
        self.add_layers_btn.setEnabled(False)
        button_layout.addWidget(self.add_layers_btn)
        
        button_layout.addStretch()
        
        self.close_btn = QPushButton("Close")
        self.close_btn.clicked.connect(self.close)
        button_layout.addWidget(self.close_btn)
        
        layout.addLayout(button_layout)
    
    def load_saved_connections(self):
        """Load saved connection settings from QSettings"""
        try:
            # Clear existing items except "New Connection..."
            self.saved_connections_combo.clear()
            self.saved_connections_combo.addItem("New Connection...")
            
            # Load saved connections
            self.settings.beginGroup("DatabricksConnector/Connections")
            connection_names = self.settings.childGroups()
            
            for conn_name in connection_names:
                self.saved_connections_combo.addItem(conn_name)
            
            self.settings.endGroup()
            
            # Load last used connection if available
            last_connection = self.settings.value("DatabricksConnector/LastConnection", "")
            if last_connection and last_connection in connection_names:
                index = self.saved_connections_combo.findText(last_connection)
                if index >= 0:
                    self.saved_connections_combo.setCurrentIndex(index)
                    self.load_selected_connection(last_connection)
                    
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error loading saved connections: {str(e)}",
                "Databricks Connector",
                Qgis.Warning
            )
    
    def load_selected_connection(self, connection_name):
        """Load selected connection details"""
        if connection_name == "New Connection..." or not connection_name:
            self.clear_connection_fields()
            return
        
        try:
            self.settings.beginGroup(f"DatabricksConnector/Connections/{connection_name}")
            
            self.connection_name_edit.setText(connection_name)
            self.hostname_edit.setText(self.settings.value("hostname", ""))
            self.http_path_edit.setText(self.settings.value("http_path", ""))
            self.access_token_edit.setText(self.settings.value("access_token", ""))
            
            self.settings.endGroup()
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error loading connection {connection_name}: {str(e)}",
                "Databricks Connector",
                Qgis.Warning
            )
    
    def clear_connection_fields(self):
        """Clear connection input fields"""
        self.connection_name_edit.clear()
        self.hostname_edit.clear()
        self.http_path_edit.clear()
        self.access_token_edit.clear()
    
    def save_current_connection(self):
        """Save current connection details"""
        connection_name = self.connection_name_edit.text().strip()
        hostname = self.hostname_edit.text().strip()
        http_path = self.http_path_edit.text().strip()
        access_token = self.access_token_edit.text().strip()
        
        if not connection_name:
            QMessageBox.warning(self, "Missing Information", 
                              "Please provide a connection name.")
            return
        
        if not all([hostname, http_path, access_token]):
            QMessageBox.warning(self, "Missing Information", 
                              "Please fill in all connection fields.")
            return
        
        try:
            # Save connection
            self.settings.beginGroup(f"DatabricksConnector/Connections/{connection_name}")
            self.settings.setValue("hostname", hostname)
            self.settings.setValue("http_path", http_path)
            self.settings.setValue("access_token", access_token)
            self.settings.endGroup()
            
            # Save as last used connection
            self.settings.setValue("DatabricksConnector/LastConnection", connection_name)
            
            # Update dropdown if it's a new connection
            if self.saved_connections_combo.findText(connection_name) < 0:
                self.saved_connections_combo.addItem(connection_name)
                self.saved_connections_combo.setCurrentText(connection_name)
            
            QMessageBox.information(self, "Connection Saved", 
                                  f"Connection '{connection_name}' saved successfully.")
            
        except Exception as e:
            QMessageBox.critical(self, "Save Failed", 
                               f"Failed to save connection: {str(e)}")
    
    def delete_saved_connection(self):
        """Delete selected saved connection"""
        current_connection = self.saved_connections_combo.currentText()
        
        if current_connection == "New Connection...":
            QMessageBox.warning(self, "No Selection", 
                              "Please select a saved connection to delete.")
            return
        
        reply = QMessageBox.question(
            self, "Confirm Delete", 
            f"Are you sure you want to delete the connection '{current_connection}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                # Remove from settings
                self.settings.remove(f"DatabricksConnector/Connections/{current_connection}")
                
                # Remove from dropdown
                index = self.saved_connections_combo.findText(current_connection)
                if index >= 0:
                    self.saved_connections_combo.removeItem(index)
                    self.saved_connections_combo.setCurrentIndex(0)  # Select "New Connection..."
                
                self.clear_connection_fields()
                
                QMessageBox.information(self, "Connection Deleted", 
                                      f"Connection '{current_connection}' deleted successfully.")
                
            except Exception as e:
                QMessageBox.critical(self, "Delete Failed", 
                                   f"Failed to delete connection: {str(e)}")
    
    def check_dependencies(self):
        """Check if required dependencies are available"""
        missing_deps = []
        
        if not DATABRICKS_AVAILABLE:
            missing_deps.append("databricks-sql-connector")
        
        if not SHAPELY_AVAILABLE:
            missing_deps.append("shapely")
        
        if missing_deps:
            self.show_dependency_error(missing_deps)
    
    def show_dependency_error(self, missing_deps):
        """Show dependency installation instructions"""
        deps_str = ", ".join(missing_deps)
        error_text = f"""
Required Python packages are missing: {deps_str}

To install the required packages:

1. Open QGIS Python Console (Plugins → Python Console)

2. Run these commands one by one:

import subprocess
import sys

{chr(10).join([f'subprocess.check_call([sys.executable, "-m", "pip", "install", "{dep}"])' for dep in missing_deps])}

3. Restart QGIS and try again.
        """.strip()
        
        # Disable connection-related controls
        self.test_connection_btn.setEnabled(False)
        self.discover_tables_btn.setEnabled(False)
        self.add_layers_btn.setEnabled(False)
        
        # Show instructions in the tables area
        self.tables_widget.hide()
        
        instructions = QPlainTextEdit()
        instructions.setPlainText(error_text)
        instructions.setReadOnly(True)
        
        # Replace table widget with instructions
        layout = self.tables_widget.parent().layout()
        layout.replaceWidget(self.tables_widget, instructions)
    
    def test_connection(self):
        """Test the database connection"""
        if not DATABRICKS_AVAILABLE:
            QMessageBox.critical(self, "Missing Dependencies", 
                               "databricks-sql-connector is not installed. Please install it first.")
            return
            
        hostname = self.hostname_edit.text().strip()
        http_path = self.http_path_edit.text().strip()
        access_token = self.access_token_edit.text().strip()
        
        if not all([hostname, http_path, access_token]):
            QMessageBox.warning(self, "Missing Information", 
                              "Please fill in all connection fields.")
            return
        
        # Show progress dialog
        self.progress_dialog = QProgressDialog("Testing connection...", "Cancel", 0, 0, self)
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.show()
        
        # Start test thread
        self.test_thread = ConnectionTestThread(hostname, http_path, access_token)
        self.test_thread.finished.connect(self.on_connection_tested)
        self.test_thread.start()
    
    def on_connection_tested(self, success, message):
        """Handle connection test results"""
        self.progress_dialog.close()
        
        if success:
            QMessageBox.information(self, "Connection Test", message)
            self.discover_tables_btn.setEnabled(True)
        else:
            QMessageBox.critical(self, "Connection Test Failed", message)
    
    def discover_tables(self):
        """Discover spatial tables in the database"""
        if not DATABRICKS_AVAILABLE:
            QMessageBox.critical(self, "Missing Dependencies", 
                               "databricks-sql-connector is not installed. Please install it first.")
            return
            
        hostname = self.hostname_edit.text().strip()
        http_path = self.http_path_edit.text().strip()
        access_token = self.access_token_edit.text().strip()
        
        if not all([hostname, http_path, access_token]):
            QMessageBox.warning(self, "Missing Information", 
                              "Please test the connection first.")
            return
        
        # Show progress dialog
        self.progress_dialog = QProgressDialog("Discovering spatial tables...", "Cancel", 0, 0, self)
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.show()
        
        # Start discovery thread
        self.discovery_thread = TableDiscoveryThread(hostname, http_path, access_token)
        self.discovery_thread.finished.connect(self.on_tables_discovered)
        self.discovery_thread.start()
    
    def on_tables_discovered(self, tables):
        """Handle discovered tables"""
        self.progress_dialog.close()
        self.tables = tables
        
        # Populate table widget
        self.tables_widget.setRowCount(len(tables))
        
        for i, table in enumerate(tables):
            # Checkbox for selection
            checkbox = QCheckBox()
            self.tables_widget.setCellWidget(i, 0, checkbox)
            
            # Table information
            self.tables_widget.setItem(i, 1, QTableWidgetItem(table['catalog']))
            self.tables_widget.setItem(i, 2, QTableWidgetItem(table['schema']))
            self.tables_widget.setItem(i, 3, QTableWidgetItem(table['table']))
            self.tables_widget.setItem(i, 4, QTableWidgetItem(table['geometry_column']))
            self.tables_widget.setItem(i, 5, QTableWidgetItem(table['geometry_type']))
        
        if tables:
            self.add_layers_btn.setEnabled(True)
            QMessageBox.information(self, "Discovery Complete", 
                                  f"Found {len(tables)} spatial table(s).")
        else:
            QMessageBox.information(self, "Discovery Complete", 
                                  "No spatial tables found in the database.")
    
    def add_selected_layers(self):
        """Add selected layers to QGIS using FIXED memory provider approach"""
        if not DATABRICKS_AVAILABLE:
            QMessageBox.critical(self, "Missing Dependencies", 
                               "databricks-sql-connector is not installed.")
            return
        
        selected_tables = []
        
        # Find selected tables
        for i in range(self.tables_widget.rowCount()):
            checkbox = self.tables_widget.cellWidget(i, 0)
            if checkbox.isChecked():
                selected_tables.append(self.tables[i])
        
        if not selected_tables:
            QMessageBox.warning(self, "No Selection", 
                              "Please select at least one table to load.")
            return
        
        # Get connection details and options
        hostname = self.hostname_edit.text().strip()
        http_path = self.http_path_edit.text().strip()
        access_token = self.access_token_edit.text().strip()
        layer_prefix = self.layer_prefix_edit.text().strip()
        
        try:
            max_features = int(self.max_features_edit.text().strip())
        except ValueError:
            max_features = 1000
        
        # Load layers one by one
        self.layers_to_load = selected_tables.copy()
        self.current_layer_index = 0
        self.loaded_layers = 0
        
        self.load_next_layer(hostname, http_path, access_token, layer_prefix, max_features)
    
    def load_next_layer(self, hostname, http_path, access_token, layer_prefix, max_features):
        """Load the next layer in the queue"""
        if self.current_layer_index >= len(self.layers_to_load):
            # All layers processed
            if self.loaded_layers > 0:
                QMessageBox.information(self, "Layers Added", 
                                      f"Successfully added {self.loaded_layers} layer(s) to QGIS.")
                
                # Save connection details for next time
                if self.connection_name_edit.text().strip():
                    self.save_current_connection()
                    
            else:
                QMessageBox.warning(self, "No Layers Added", 
                                  "No layers were successfully added. Check the message log for details.")
            return
        
        table = self.layers_to_load[self.current_layer_index]
        layer_name = f"{layer_prefix}{table['table']}"
        
        # Show progress dialog
        self.progress_dialog = QProgressDialog(f"Loading layer: {layer_name}", "Cancel", 0, 0, self)
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.show()
        
        # Start loading thread
        self.loading_thread = LayerLoadingThread(
            hostname, http_path, access_token, table, layer_name, max_features
        )
        self.loading_thread.progress.connect(self.on_loading_progress)
        self.loading_thread.finished.connect(self.on_layer_loaded)
        self.loading_thread.start()
    
    def on_loading_progress(self, message):
        """Update progress dialog"""
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.setLabelText(message)
    
    def on_layer_loaded(self, success, message, layer):
        """Handle layer loading results"""
        self.progress_dialog.close()
        
        if success and layer:
            # CRITICAL: Check layer validity before adding
            if layer.isValid():
                # Add layer to QGIS project
                QgsProject.instance().addMapLayer(layer)
                self.loaded_layers += 1
                
                QgsMessageLog.logMessage(
                    f"Successfully added layer: {layer.name()} with {layer.featureCount()} features",
                    "Databricks Connector",
                    Qgis.Info
                )
                
                # Zoom to layer extent if it has features
                if layer.featureCount() > 0:
                    self.iface.mapCanvas().setExtent(layer.extent())
                    self.iface.mapCanvas().refresh()
                
                # Check if we need to create additional layers for mixed geometries
                # We need to check the table_info from the loading thread, not the original table
                if hasattr(self.loading_thread, 'table_info'):
                    thread_table_info = self.loading_thread.table_info
                    if (thread_table_info.get('mixed_geometries', False) and 
                        'geometry_types_list' in thread_table_info):
                        
                        # Create additional layers for LineStrings and Polygons
                        self.create_additional_geometry_layers(thread_table_info)
                
            else:
                QgsMessageLog.logMessage(
                    f"Layer is invalid: {message}",
                    "Databricks Connector",
                    Qgis.Critical
                )
        else:
            QgsMessageLog.logMessage(
                f"Failed to load layer: {message}",
                "Databricks Connector",
                Qgis.Critical
            )
        
        # Move to next layer
        self.current_layer_index += 1
        
        # Get connection details for next layer
        hostname = self.hostname_edit.text().strip()
        http_path = self.http_path_edit.text().strip()
        access_token = self.access_token_edit.text().strip()
        layer_prefix = self.layer_prefix_edit.text().strip()
        
        try:
            max_features = int(self.max_features_edit.text().strip())
        except ValueError:
            max_features = 1000
        
        # Load next layer
        self.load_next_layer(hostname, http_path, access_token, layer_prefix, max_features)
    
    def create_additional_geometry_layers(self, table_info):
        """Create additional layers for LineStrings and Polygons in mixed geometry tables"""
        try:
            geometry_types = table_info.get('geometry_types_list', [])
            
            # Get connection details
            hostname = self.hostname_edit.text().strip()
            http_path = self.http_path_edit.text().strip()
            access_token = self.access_token_edit.text().strip()
            layer_prefix = self.layer_prefix_edit.text().strip()
            
            try:
                max_features = int(self.max_features_edit.text().strip())
            except ValueError:
                max_features = 1000
            
            # Create layers for LineStrings and Polygons
            for geom_type in geometry_types:
                if geom_type in ['ST_LINESTRING', 'ST_POLYGON']:
                    # Create a modified table_info for this specific geometry type
                    specific_table_info = table_info.copy()
                    specific_table_info['geometry_type'] = geom_type
                    specific_table_info['mixed_geometries'] = False
                    specific_table_info['target_geometry_type'] = geom_type  # Filter for this type only
                    
                    # Create layer name with geometry type suffix
                    geom_suffix = "lines" if geom_type == 'ST_LINESTRING' else "polygons"
                    layer_name = f"{layer_prefix}{table_info['table']}_{geom_suffix}"
                    
                    QgsMessageLog.logMessage(
                        f"Creating additional layer for {geom_type}: {layer_name}",
                        "Databricks Connector",
                        Qgis.Info
                    )
                    
                    # Start loading thread for this geometry type
                    loading_thread = LayerLoadingThread(
                        hostname, http_path, access_token, specific_table_info, layer_name, max_features
                    )
                    loading_thread.progress.connect(self.on_loading_progress)
                    loading_thread.finished.connect(self.on_additional_layer_loaded)
                    loading_thread.start()
                    
                    # Store reference to prevent garbage collection
                    if not hasattr(self, 'additional_threads'):
                        self.additional_threads = []
                    self.additional_threads.append(loading_thread)
        
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error creating additional geometry layers: {str(e)}",
                "Databricks Connector",
                Qgis.Critical
            )
    
    def on_additional_layer_loaded(self, success, message, layer):
        """Handle additional layer loading results"""
        if success and layer and layer.isValid():
            # Add layer to QGIS project
            QgsProject.instance().addMapLayer(layer)
            self.loaded_layers += 1
            
            QgsMessageLog.logMessage(
                f"Successfully added additional layer: {layer.name()} with {layer.featureCount()} features",
                "Databricks Connector",
                Qgis.Info
            )
        else:
            QgsMessageLog.logMessage(
                f"Failed to load additional layer: {message}",
                "Databricks Connector",
                Qgis.Warning
            )
    
    def open_custom_query(self):
        """Open the custom query dialog"""
        if not QUERY_DIALOG_AVAILABLE:
            error_msg = "Custom query dialog is not available due to missing dependencies."
            if QUERY_DIALOG_IMPORT_ERROR:
                error_msg += f"\n\nError details: {QUERY_DIALOG_IMPORT_ERROR}"
            QMessageBox.critical(self, "Feature Not Available", error_msg)
            return
        
        # Get current connection details
        hostname = self.hostname_edit.text().strip()
        http_path = self.http_path_edit.text().strip()
        access_token = self.access_token_edit.text().strip()
        
        if not all([hostname, http_path, access_token]):
            QMessageBox.warning(self, "Missing Connection", 
                              "Please test the connection first or fill in all connection fields.")
            return
        
        try:
            connection_config = {
                'hostname': hostname,
                'http_path': http_path,
                'access_token': access_token
            }
            
            query_dialog = DatabricksQueryDialog(connection_config, self)
            query_dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(self, "Error Opening Query Dialog", 
                               f"Failed to open custom query dialog: {str(e)}")
    
    def create_databricks_layer(self, table_info, layer_name, connection_config):
        """Create a layer using the Databricks provider for persistence"""
        try:
            # Create URI for the Databricks provider
            uri = self._create_provider_uri(table_info, connection_config)
            
            # Create vector layer using the Databricks provider
            layer = QgsVectorLayer(uri, layer_name, "databricks")
            
            if layer.isValid():
                return layer
            else:
                QgsMessageLog.logMessage(
                    f"Failed to create Databricks provider layer: {layer.error().message()}",
                    "Databricks Connector",
                    Qgis.Warning
                )
                return None
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error creating Databricks provider layer: {str(e)}",
                "Databricks Connector",
                Qgis.Warning
            )
            return None
    
    def _create_provider_uri(self, table_info, connection_config):
        """Create URI for the Databricks provider"""
        # Properly encode the URI
        import urllib.parse
        
        hostname = connection_config['hostname']
        http_path = connection_config['http_path']
        access_token = connection_config['access_token']
        
        base_uri = f"databricks://{hostname}:443{http_path}"
        
        params = {
            'access_token': access_token,
            'table': table_info['full_name']
        }
        
        if table_info.get('geometry_column'):
            params['geom_column'] = table_info['geometry_column']
        
        query_string = urllib.parse.urlencode(params)
        return f"{base_uri}?{query_string}"


# ===== QUERY DIALOG CLASSES =====

class DatabaseStructureThread(QThread):
    """Thread for loading database structure (catalogs, schemas, tables)"""
    
    progress = pyqtSignal(str)  # progress message
    finished = pyqtSignal(dict)  # database structure dict
    
    def __init__(self, connection_config):
        super().__init__()
        self.connection_config = connection_config
    
    def run(self):
        if not DATABRICKS_AVAILABLE:
            self.finished.emit({})
            return
        
        structure = {}
        
        try:
            self.progress.emit("Loading database structure...")
            
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            with connection.cursor() as cursor:
                # Use information_schema to get only accessible tables and columns
                self.progress.emit("Loading accessible database structure...")
                
                # Query information_schema to get all accessible tables and columns
                info_query = """
                    SELECT 
                        table_catalog,
                        table_schema, 
                        table_name,
                        column_name,
                        data_type
                    FROM information_schema.columns 
                    WHERE table_catalog IS NOT NULL 
                        AND table_schema IS NOT NULL
                        AND table_name IS NOT NULL
                    ORDER BY table_catalog, table_schema, table_name, ordinal_position
                """
                
                QgsMessageLog.logMessage(
                    f"Querying accessible database structure with: {info_query}",
                    "Query Dialog",
                    Qgis.Info
                )
                
                cursor.execute(info_query)
                results = cursor.fetchall()
                
                QgsMessageLog.logMessage(
                    f"Found {len(results)} accessible columns across all tables",
                    "Query Dialog",
                    Qgis.Info
                )
                
                # Group results by catalog/schema/table
                for row in results:
                    catalog = row[0]
                    schema = row[1] 
                    table = row[2]
                    column_name = row[3]
                    data_type = row[4]
                    
                    # Initialize nested structure
                    if catalog not in structure:
                        structure[catalog] = {}
                    if schema not in structure[catalog]:
                        structure[catalog][schema] = {}
                    if table not in structure[catalog][schema]:
                        structure[catalog][schema][table] = {
                            'columns': [],
                            'full_name': f"{catalog}.{schema}.{table}"
                        }
                    
                    # Add column info
                    structure[catalog][schema][table]['columns'].append({
                        'name': column_name,
                        'type': data_type,
                        'is_geometry': data_type.upper() in ['GEOMETRY', 'GEOGRAPHY']
                    })
                
                total_catalogs = len(structure)
                total_schemas = sum(len(schemas) for schemas in structure.values())
                total_tables = sum(len(tables) for catalog in structure.values() for tables in catalog.values())
                
                QgsMessageLog.logMessage(
                    f"Loaded {total_catalogs} catalogs, {total_schemas} schemas, {total_tables} tables",
                    "Query Dialog",
                    Qgis.Info
                )
            
            connection.close()
            
            self.progress.emit("Database structure loaded!")
            self.finished.emit(structure)
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error loading database structure: {str(e)}",
                "Query Dialog",
                Qgis.Critical
            )
            self.finished.emit({})


class QueryExecutionThread(QThread):
    """Thread for executing SQL queries"""
    
    progress = pyqtSignal(str)  # progress message
    finished = pyqtSignal(bool, str, list, list)  # success, message, columns, rows
    
    def __init__(self, connection_config, query):
        super().__init__()
        self.connection_config = connection_config
        self.query = query
    
    def run(self):
        if not DATABRICKS_AVAILABLE:
            self.finished.emit(False, "databricks-sql-connector not installed", [], [])
            return
        
        try:
            self.progress.emit("Connecting to Databricks...")
            
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            self.progress.emit("Executing query...")
            
            with connection.cursor() as cursor:
                cursor.execute(self.query)
                
                # Get column information
                columns = []
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                
                # Fetch results
                rows = cursor.fetchall()
            
            connection.close()
            
            self.finished.emit(True, f"Query executed successfully. {len(rows)} rows returned.", columns, rows)
            
        except Exception as e:
            self.finished.emit(False, f"Query failed: {str(e)}", [], [])


class QueryLayerCreationThread(QThread):
    """Thread for creating layers from query results"""
    
    progress = pyqtSignal(str)  # progress message
    finished = pyqtSignal(bool, str, object)  # success, message, layer
    
    def __init__(self, connection_config, query, layer_name, geometry_column=None):
        super().__init__()
        self.connection_config = connection_config
        self.query = query
        self.layer_name = layer_name
        self.geometry_column = geometry_column
    
    def run(self):
        if not DATABRICKS_AVAILABLE:
            self.finished.emit(False, "databricks-sql-connector not installed", None)
            return
        
        try:
            self.progress.emit("Connecting to Databricks...")
            
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            self.progress.emit("Analyzing query for geometry columns...")
            
            # First, check if we need to modify the query for geometry conversion
            modified_query = self._add_geometry_conversion(connection, self.query)
            
            self.progress.emit("Executing query...")
            
            with connection.cursor() as cursor:
                cursor.execute(modified_query)
                
                # Get column information
                columns = []
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                
                # Fetch results
                rows = cursor.fetchall()
            
            connection.close()
            
            if not rows:
                self.finished.emit(False, "Query returned no results", None)
                return
            
            self.progress.emit("Creating QGIS layer...")
            
            # Determine geometry column if not specified or validate if specified
            if self.geometry_column:
                # User specified a geometry column - validate it exists
                if self.geometry_column not in columns:
                    QgsMessageLog.logMessage(
                        f"User-specified geometry column '{self.geometry_column}' not found in query results. Available columns: {columns}",
                        "Query Dialog",
                        Qgis.Warning
                    )
                    self.geometry_column = None  # Reset and try auto-detection
                else:
                    QgsMessageLog.logMessage(
                        f"Using user-specified geometry column: {self.geometry_column}",
                        "Query Dialog",
                        Qgis.Info
                    )
            
            if not self.geometry_column:
                # First, check if any column contains WKT data by examining sample values
                sample_row = rows[0] if rows else None
                
                if sample_row:
                    for i, col in enumerate(columns):
                        if i < len(sample_row):
                            sample_value = sample_row[i]
                            if sample_value and self._is_wkt_format(str(sample_value)):
                                self.geometry_column = col
                                # Show the cleaned WKT in logs (without SRID)
                                clean_sample = self._strip_srid_from_wkt(str(sample_value))
                                QgsMessageLog.logMessage(
                                    f"Auto-detected WKT geometry column: {col} (contains: {clean_sample[:50]}...)",
                                    "Query Dialog",
                                    Qgis.Info
                                )
                                break
                
                # If still not found, look for common geometry column names
                if not self.geometry_column:
                    geom_candidates = ['geometry', 'geom', 'location', 'point', 'polygon', 'linestring', 'shape', 'spatial']
                    for col in columns:
                        if col.lower() in geom_candidates:
                            self.geometry_column = col
                            QgsMessageLog.logMessage(
                                f"Auto-detected geometry column by name: {col}",
                                "Query Dialog",
                                Qgis.Info
                            )
                            break
            
            if not self.geometry_column:
                QgsMessageLog.logMessage(
                    f"No geometry column detected. Available columns: {columns}. Layer will be created without geometry.",
                    "Query Dialog",
                    Qgis.Info
                )
            
            # Create fields for non-geometry columns
            fields = QgsFields()
            geom_col_index = None
            
            for i, col in enumerate(columns):
                if col.lower() == (self.geometry_column or '').lower():
                    geom_col_index = i
                    QgsMessageLog.logMessage(
                        f"Found geometry column '{col}' at index {i}",
                        "Query Dialog",
                        Qgis.Info
                    )
                else:
                    # Determine field type from first non-null value
                    field_type = QVariant.String  # default
                    for row in rows:
                        if row[i] is not None:
                            if isinstance(row[i], int):
                                field_type = QVariant.LongLong
                            elif isinstance(row[i], float):
                                field_type = QVariant.Double
                            elif isinstance(row[i], bool):
                                field_type = QVariant.Bool
                            break
                    
                    field = QgsField(col, field_type)
                    fields.append(field)
                    QgsMessageLog.logMessage(
                        f"Added attribute field: {col} ({field_type})",
                        "Query Dialog",
                        Qgis.Info
                    )
            
            # Determine geometry types from all geometries and handle mixed types
            geometry_types_in_data = set()
            if geom_col_index is not None and rows:
                for row in rows:
                    if geom_col_index < len(row) and row[geom_col_index]:
                        geom_wkt = str(row[geom_col_index])
                        # Strip SRID prefix before checking geometry type
                        clean_wkt = self._strip_srid_from_wkt(geom_wkt).strip().upper()
                        
                        if clean_wkt.startswith('POINT'):
                            geometry_types_in_data.add('Point')
                        elif clean_wkt.startswith('LINESTRING'):
                            geometry_types_in_data.add('LineString')
                        elif clean_wkt.startswith('POLYGON'):
                            geometry_types_in_data.add('Polygon')
                        elif clean_wkt.startswith('MULTIPOINT'):
                            geometry_types_in_data.add('MultiPoint')
                        elif clean_wkt.startswith('MULTILINESTRING'):
                            geometry_types_in_data.add('MultiLineString')
                        elif clean_wkt.startswith('MULTIPOLYGON'):
                            geometry_types_in_data.add('MultiPolygon')
            
            QgsMessageLog.logMessage(
                f"Detected geometry types in query results: {list(geometry_types_in_data)}",
                "Query Dialog",
                Qgis.Info
            )
            
            # Check if we have mixed geometry types
            if len(geometry_types_in_data) > 1:
                QgsMessageLog.logMessage(
                    f"Mixed geometry types detected: {list(geometry_types_in_data)}. Creating separate layers for each type.",
                    "Query Dialog",
                    Qgis.Info
                )
                # Create separate layers for each geometry type
                self._create_mixed_geometry_layers(columns, rows, fields, geom_col_index, geometry_types_in_data)
                return
            
            # Single geometry type or no geometry
            geom_type = list(geometry_types_in_data)[0] if geometry_types_in_data else "Point"
            
            if geom_col_index is not None and rows:
                first_geom = rows[0][geom_col_index]
                if first_geom:
                    try:
                        if SHAPELY_AVAILABLE:
                            shapely_geom = wkt.loads(str(first_geom))
                            if shapely_geom.geom_type == 'LineString' or shapely_geom.geom_type == 'MultiLineString':
                                geom_type = "LineString"
                            elif shapely_geom.geom_type == 'Polygon' or shapely_geom.geom_type == 'MultiPolygon':
                                geom_type = "Polygon"
                        else:
                            # Fallback to simple string matching
                            geom_str = str(first_geom).upper()
                            if 'LINESTRING' in geom_str or 'MULTILINESTRING' in geom_str:
                                geom_type = "LineString"
                            elif 'POLYGON' in geom_str or 'MULTIPOLYGON' in geom_str:
                                geom_type = "Polygon"
                    except:
                        pass  # Keep default Point type
            
            # Create memory layer
            if geom_col_index is not None:
                layer_def = f"{geom_type}?crs=EPSG:4326"
                memory_layer = QgsVectorLayer(layer_def, self.layer_name, "memory")
            else:
                # No geometry, create attribute-only layer
                memory_layer = QgsVectorLayer("None", self.layer_name, "memory")
            
            if not memory_layer.isValid():
                self.finished.emit(False, f"Failed to create memory layer", None)
                return
            
            # Add fields
            memory_layer.startEditing()
            provider = memory_layer.dataProvider()
            provider.addAttributes(fields.toList())
            memory_layer.updateFields()
            
            self.progress.emit(f"Adding {len(rows)} features...")
            
            # Add features
            features_to_add = []
            successful_geometries = 0
            
            QgsMessageLog.logMessage(
                f"Processing {len(rows)} rows. Geometry column index: {geom_col_index}",
                "Query Dialog",
                Qgis.Info
            )
            
            for i, row in enumerate(rows):
                feature = QgsFeature(memory_layer.fields(), i + 1)
                
                # Set attributes (excluding geometry column)
                attrs = []
                for j, value in enumerate(row):
                    if j != geom_col_index:
                        attrs.append(value)
                
                feature.setAttributes(attrs)
                
                # Set geometry if present
                if geom_col_index is not None and geom_col_index < len(row) and row[geom_col_index]:
                    try:
                        geom_wkt = str(row[geom_col_index])
                        
                        QgsMessageLog.logMessage(
                            f"Feature {i}: Processing geometry WKT: {geom_wkt[:100]}...",
                            "Query Dialog",
                            Qgis.Info
                        )
                        
                        # Strip SRID prefix before parsing
                        clean_wkt = self._strip_srid_from_wkt(geom_wkt)
                        
                        if clean_wkt != geom_wkt:
                            QgsMessageLog.logMessage(
                                f"Feature {i}: Stripped SRID prefix: {clean_wkt[:100]}...",
                                "Query Dialog",
                                Qgis.Info
                            )
                        
                        # Parse geometry using QGIS built-in WKT parser
                        geometry = QgsGeometry.fromWkt(clean_wkt)
                        
                        if not geometry.isNull() and geometry.isGeosValid():
                            feature.setGeometry(geometry)
                            successful_geometries += 1
                            QgsMessageLog.logMessage(
                                f"Feature {i}: Successfully set geometry",
                                "Query Dialog",
                                Qgis.Info
                            )
                        else:
                            QgsMessageLog.logMessage(
                                f"Feature {i}: Invalid geometry after SRID stripping: {clean_wkt[:100]}...",
                                "Query Dialog",
                                Qgis.Warning
                            )
                        
                    except Exception as e:
                        QgsMessageLog.logMessage(
                            f"Feature {i}: Error parsing geometry: {str(e)}, WKT: {geom_wkt[:100]}...",
                            "Query Dialog",
                            Qgis.Warning
                        )
                else:
                    if geom_col_index is not None:
                        QgsMessageLog.logMessage(
                            f"Feature {i}: No geometry data (geom_col_index={geom_col_index}, row_len={len(row)}, value={row[geom_col_index] if geom_col_index < len(row) else 'N/A'})",
                            "Query Dialog",
                            Qgis.Info
                        )
                
                features_to_add.append(feature)
            
            QgsMessageLog.logMessage(
                f"Created {len(features_to_add)} features, {successful_geometries} with valid geometries",
                "Query Dialog",
                Qgis.Info
            )
            
            # Add features to layer
            provider.addFeatures(features_to_add)
            memory_layer.commitChanges()
            memory_layer.updateExtents()
            
            # Check if we had geometry issues and inform the user
            total_features = len(rows)
            successful_features = memory_layer.featureCount()
            
            QgsMessageLog.logMessage(
                f"Layer creation summary: {successful_features} features in layer out of {total_features} rows processed. Geometry column: {self.geometry_column}, Geometry column index: {geom_col_index}, Successful geometries: {successful_geometries}",
                "Query Dialog",
                Qgis.Info
            )
            
            if successful_features < total_features:
                message = f"Created layer with {successful_features} features out of {total_features} rows. "
                message += f"{total_features - successful_features} features were skipped due to geometry parsing issues. "
                message += "Check the log for details."
            else:
                message = f"Created layer with {successful_features} features"
            
            self.finished.emit(True, message, memory_layer)
            
        except Exception as e:
            self.finished.emit(False, f"Error creating layer: {str(e)}", None)
    
    def _create_mixed_geometry_layers(self, columns, rows, fields, geom_col_index, geometry_types):
        """Create separate layers for each geometry type in mixed geometry data"""
        try:
            created_layers = []
            
            for geom_type in sorted(geometry_types):
                QgsMessageLog.logMessage(
                    f"Creating layer for geometry type: {geom_type}",
                    "Query Dialog",
                    Qgis.Info
                )
                
                # Filter rows for this specific geometry type
                filtered_rows = []
                for row in rows:
                    if geom_col_index < len(row) and row[geom_col_index]:
                        geom_wkt = str(row[geom_col_index])
                        # Strip SRID prefix before checking geometry type
                        clean_wkt = self._strip_srid_from_wkt(geom_wkt).strip().upper()
                        row_geom_type = None
                        
                        if clean_wkt.startswith('POINT'):
                            row_geom_type = 'Point'
                        elif clean_wkt.startswith('LINESTRING'):
                            row_geom_type = 'LineString'
                        elif clean_wkt.startswith('POLYGON'):
                            row_geom_type = 'Polygon'
                        elif clean_wkt.startswith('MULTIPOINT'):
                            row_geom_type = 'MultiPoint'
                        elif clean_wkt.startswith('MULTILINESTRING'):
                            row_geom_type = 'MultiLineString'
                        elif clean_wkt.startswith('MULTIPOLYGON'):
                            row_geom_type = 'MultiPolygon'
                        
                        if row_geom_type == geom_type:
                            filtered_rows.append(row)
                
                if not filtered_rows:
                    QgsMessageLog.logMessage(
                        f"No features found for geometry type: {geom_type}, skipping",
                        "Query Dialog",
                        Qgis.Info
                    )
                    continue
                
                QgsMessageLog.logMessage(
                    f"Creating {geom_type} layer with {len(filtered_rows)} filtered rows",
                    "Query Dialog",
                    Qgis.Info
                )
                
                # Create layer for this geometry type - SIMPLE VERSION
                layer = self._create_simple_layer(
                    f"Databricks_Query_{geom_type}", 
                    geom_type, 
                    filtered_rows, 
                    fields, 
                    geom_col_index
                )
                
                if layer and layer.featureCount() > 0:
                    created_layers.append(layer)
                    QgsMessageLog.logMessage(
                        f"Successfully created {geom_type} layer with {layer.featureCount()} features",
                        "Query Dialog",
                        Qgis.Info
                    )
                else:
                    QgsMessageLog.logMessage(
                        f"Failed to create {geom_type} layer or layer has 0 features",
                        "Query Dialog",
                        Qgis.Warning
                    )
            
            if created_layers:
                # Add all layers to QGIS
                for layer in created_layers:
                    QgsProject.instance().addMapLayer(layer)
                
                total_features = sum(layer.featureCount() for layer in created_layers)
                message = f"Created {len(created_layers)} layers with {total_features} total features: "
                message += ", ".join([f"{layer.name()} ({layer.featureCount()})" for layer in created_layers])
                
                self.finished.emit(True, message, created_layers[0])
            else:
                self.finished.emit(False, "No valid layers created from mixed geometry data", None)
                
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error creating mixed geometry layers: {str(e)}",
                "Query Dialog",
                Qgis.Critical
            )
            self.finished.emit(False, f"Error creating mixed geometry layers: {str(e)}", None)
    
    def _create_simple_layer(self, layer_name, geom_type, filtered_rows, fields, geom_col_index):
        """Create a simple layer - MINIMAL WORKING VERSION"""
        try:
            QgsMessageLog.logMessage(
                f"Creating MINIMAL {geom_type} layer '{layer_name}' with {len(filtered_rows)} rows",
                "Query Dialog", Qgis.Info
            )
            
            # Create memory layer
            layer_def = f"{geom_type}?crs=EPSG:4326"
            memory_layer = QgsVectorLayer(layer_def, layer_name, "memory")
            
            if not memory_layer.isValid():
                QgsMessageLog.logMessage(f"Failed to create memory layer: {layer_def}", "Query Dialog", Qgis.Critical)
                return None
            
            # CRITICAL: Get provider and start editing BEFORE adding fields
            provider = memory_layer.dataProvider()
            memory_layer.startEditing()
            
            # Add ONLY the non-geometry fields 
            non_geom_fields = QgsFields()
            for field in fields:
                non_geom_fields.append(field)
            
            QgsMessageLog.logMessage(f"Adding {non_geom_fields.count()} fields to layer", "Query Dialog", Qgis.Info)
            
            # Add attributes to provider
            add_result = provider.addAttributes(non_geom_fields.toList())
            QgsMessageLog.logMessage(f"AddAttributes result: {add_result}", "Query Dialog", Qgis.Info)
            
            # Update fields
            memory_layer.updateFields()
            QgsMessageLog.logMessage(f"Layer fields after update: {memory_layer.fields().count()}", "Query Dialog", Qgis.Info)
            
            # Process filtered rows only
            features_to_add = []
            
            for i, row in enumerate(filtered_rows):
                # Create feature with correct field structure
                feature = QgsFeature(memory_layer.fields())
                
                # Set attributes - CRITICAL: match field count exactly
                attrs = []
                attr_index = 0
                for j, value in enumerate(row):
                    if j != geom_col_index:  # Skip geometry column
                        attrs.append(value)
                        attr_index += 1
                
                QgsMessageLog.logMessage(
                    f"Feature {i}: Setting {len(attrs)} attributes for {memory_layer.fields().count()} fields",
                    "Query Dialog", Qgis.Info
                )
                
                feature.setAttributes(attrs)
                
                # Set geometry
                if geom_col_index is not None and geom_col_index < len(row) and row[geom_col_index]:
                    geom_wkt = str(row[geom_col_index])
                    # Strip SRID prefix before parsing
                    clean_wkt = self._strip_srid_from_wkt(geom_wkt)
                    geometry = QgsGeometry.fromWkt(clean_wkt)
                    
                    if not geometry.isNull() and geometry.isGeosValid():
                        feature.setGeometry(geometry)
                        QgsMessageLog.logMessage(f"Feature {i}: Geometry set successfully", "Query Dialog", Qgis.Info)
                    else:
                        QgsMessageLog.logMessage(f"Feature {i}: Invalid geometry after SRID stripping: {clean_wkt[:100]}...", "Query Dialog", Qgis.Warning)
                
                features_to_add.append(feature)
            
            QgsMessageLog.logMessage(f"About to add {len(features_to_add)} features to layer using WORKING METHOD", "Query Dialog", Qgis.Info)
            
            # COPY EXACT WORKING METHOD FROM LayerLoadingThread
            successful_adds = 0
            
            # Method 1: Try using layer.addFeature() instead of dataProvider().addFeatures()
            QgsMessageLog.logMessage("Trying Method 1: layer.addFeature()", "Query Dialog", Qgis.Info)
            
            for i, feature in enumerate(features_to_add):
                try:
                    add_result = memory_layer.addFeature(feature)
                    if add_result:
                        successful_adds += 1
                        QgsMessageLog.logMessage(f"Successfully added feature {i} using layer.addFeature", "Query Dialog", Qgis.Info)
                    else:
                        QgsMessageLog.logMessage(f"Failed to add feature {i} using layer.addFeature, trying Method 2", "Query Dialog", Qgis.Warning)
                        
                        # Method 2: Try with dataProvider if layer method fails
                        QgsMessageLog.logMessage(f"Trying Method 2 for feature {i}: dataProvider.addFeatures()", "Query Dialog", Qgis.Info)
                        
                        single_result = memory_layer.dataProvider().addFeatures([feature])
                        if single_result[0]:
                            successful_adds += 1
                            QgsMessageLog.logMessage(f"Successfully added feature {i} using dataProvider", "Query Dialog", Qgis.Info)
                        else:
                            QgsMessageLog.logMessage(f"Failed to add feature {i} using both methods", "Query Dialog", Qgis.Critical)
                
                except Exception as e:
                    QgsMessageLog.logMessage(f"Exception adding feature {i}: {str(e)}", "Query Dialog", Qgis.Critical)
            
            QgsMessageLog.logMessage(f"Successfully added {successful_adds} out of {len(features_to_add)} features", "Query Dialog", Qgis.Info)
            
            # Commit and check final count
            commit_result = memory_layer.commitChanges()
            QgsMessageLog.logMessage(f"CommitChanges result: {commit_result}", "Query Dialog", Qgis.Info)
            
            memory_layer.updateExtents()
            
            final_count = memory_layer.featureCount()
            QgsMessageLog.logMessage(f"FINAL: Layer has {final_count} features", "Query Dialog", Qgis.Info)
            
            if final_count > 0:
                return memory_layer
            else:
                QgsMessageLog.logMessage(f"Layer creation failed - 0 features", "Query Dialog", Qgis.Critical)
                return None
            
        except Exception as e:
            QgsMessageLog.logMessage(f"Error creating simple layer {geom_type}: {str(e)}", "Query Dialog", Qgis.Critical)
            import traceback
            QgsMessageLog.logMessage(f"Traceback: {traceback.format_exc()}", "Query Dialog", Qgis.Critical)
            return None
    
    def _create_single_geometry_layer(self, layer_name, geom_type, columns, rows, fields, geom_col_index):
        """Create a single layer for specific geometry type"""
        try:
            # Create memory layer
            memory_layer = QgsVectorLayer(f"{geom_type}?crs=EPSG:4326", layer_name, "memory")
            
            if not memory_layer.isValid():
                QgsMessageLog.logMessage(
                    f"Failed to create memory layer for {geom_type}",
                    "Query Dialog",
                    Qgis.Critical
                )
                return None
            
            # Add fields
            provider = memory_layer.dataProvider()
            provider.addAttributes(fields.toList())
            memory_layer.updateFields()
            
            # Add features - ONLY add features that match this geometry type
            features_to_add = []
            successful_geometries = 0
            
            for i, row in enumerate(rows):
                # Check if this row's geometry matches the target geometry type
                if geom_col_index is not None and geom_col_index < len(row) and row[geom_col_index]:
                    try:
                        geom_wkt = str(row[geom_col_index]).strip().upper()
                        
                        # Determine this row's geometry type
                        row_geom_type = None
                        if geom_wkt.startswith('POINT'):
                            row_geom_type = 'Point'
                        elif geom_wkt.startswith('LINESTRING'):
                            row_geom_type = 'LineString'
                        elif geom_wkt.startswith('POLYGON'):
                            row_geom_type = 'Polygon'
                        elif geom_wkt.startswith('MULTIPOINT'):
                            row_geom_type = 'MultiPoint'
                        elif geom_wkt.startswith('MULTILINESTRING'):
                            row_geom_type = 'MultiLineString'
                        elif geom_wkt.startswith('MULTIPOLYGON'):
                            row_geom_type = 'MultiPolygon'
                        
                        # Only process features that match the target geometry type
                        if row_geom_type == geom_type:
                            feature = QgsFeature(memory_layer.fields(), len(features_to_add) + 1)
                            
                            # Set attributes (excluding geometry column)
                            attrs = []
                            for j, value in enumerate(row):
                                if j != geom_col_index:
                                    attrs.append(value)
                            
                            feature.setAttributes(attrs)
                            
                            # Set geometry
                            geometry = QgsGeometry.fromWkt(geom_wkt)
                            if not geometry.isNull() and geometry.isGeosValid():
                                feature.setGeometry(geometry)
                                successful_geometries += 1
                                features_to_add.append(feature)
                                
                                QgsMessageLog.logMessage(
                                    f"Added {geom_type} feature {len(features_to_add)}: {geom_wkt[:50]}...",
                                    "Query Dialog",
                                    Qgis.Info
                                )
                            else:
                                QgsMessageLog.logMessage(
                                    f"Invalid {geom_type} geometry skipped: {geom_wkt[:100]}...",
                                    "Query Dialog",
                                    Qgis.Warning
                                )
                        
                    except Exception as e:
                        QgsMessageLog.logMessage(
                            f"Error processing feature {i} for {geom_type}: {str(e)}",
                            "Query Dialog",
                            Qgis.Warning
                        )
            
            # Add features to layer
            provider.addFeatures(features_to_add)
            memory_layer.commitChanges()
            memory_layer.updateExtents()
            
            QgsMessageLog.logMessage(
                f"Created {geom_type} layer: {memory_layer.featureCount()} features, {successful_geometries} with valid geometries",
                "Query Dialog",
                Qgis.Info
            )
            
            return memory_layer
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error creating single geometry layer {geom_type}: {str(e)}",
                "Query Dialog",
                Qgis.Critical
            )
            return None
    
    def _is_wkt_format(self, value_str):
        """Check if a string is already in WKT format (handles SRID prefixes)"""
        if not isinstance(value_str, str):
            return False
        
        value_str = value_str.strip().upper()
        
        # Handle SRID prefixes (e.g., "SRID=4326;POINT(...)")
        if value_str.startswith('SRID='):
            # Extract WKT part after the semicolon
            srid_parts = value_str.split(';', 1)
            if len(srid_parts) > 1:
                value_str = srid_parts[1].strip()
        
        # Check for common WKT prefixes
        wkt_prefixes = ['POINT', 'LINESTRING', 'POLYGON', 'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON', 'GEOMETRYCOLLECTION']
        
        is_wkt = any(value_str.startswith(prefix) for prefix in wkt_prefixes)
        
        if is_wkt:
            QgsMessageLog.logMessage(
                f"Detected WKT format: {value_str[:50]}...",
                "Query Dialog",
                Qgis.Info
            )
        
        return is_wkt
    
    def _strip_srid_from_wkt(self, wkt_str):
        """Strip SRID prefix from WKT string (e.g., 'SRID=4326;POINT(...)' → 'POINT(...)')"""
        if not isinstance(wkt_str, str):
            return wkt_str
        
        wkt_str = wkt_str.strip()
        
        # Handle SRID prefixes (e.g., "SRID=4326;POINT(...)")
        if wkt_str.upper().startswith('SRID='):
            # Extract WKT part after the semicolon
            srid_parts = wkt_str.split(';', 1)
            if len(srid_parts) > 1:
                return srid_parts[1].strip()
        
        return wkt_str
    
    def _looks_like_geometry_column(self, column_name, sample_value):
        """Check if a column looks like it contains geometry data"""
        # Check column name
        geom_names = ['geometry', 'geom', 'location', 'point', 'polygon', 'linestring', 'shape', 'spatial']
        if any(name in column_name.lower() for name in geom_names):
            return True
        
        # Check if sample value looks like geometry (either WKT or Databricks format)
        if sample_value is None:
            return False
        
        value_str = str(sample_value).strip()
        
        # Check for WKT format
        if self._is_wkt_format(value_str):
            return True
        
        # Check for Databricks binary geometry format (typically starts with specific bytes)
        # This is a heuristic - Databricks geometry might be in binary format
        if len(value_str) > 10 and any(c in value_str.lower() for c in ['point', 'line', 'polygon']):
            return True
        
        return False
    
    def _add_geometry_conversion(self, connection, query):
        """Automatically add ST_ASWKT conversion for GEOMETRY/GEOGRAPHY columns"""
        try:
            # Simple check: if query already contains ST_ASWKT or ST_ASTEXT, don't modify
            query_upper = query.upper()
            if 'ST_ASWKT' in query_upper or 'ST_ASTEXT' in query_upper:
                QgsMessageLog.logMessage(
                    "Query already contains geometry conversion functions, using as-is",
                    "Query Dialog",
                    Qgis.Info
                )
                return query
            
            # Extract table names from the query using a simple regex approach
            import re
            
            # Find FROM clause and extract table names
            from_match = re.search(r'\bFROM\s+([^\s,]+(?:\s*,\s*[^\s,]+)*)', query, re.IGNORECASE)
            if not from_match:
                QgsMessageLog.logMessage(
                    "Could not find FROM clause in query, using query as-is",
                    "Query Dialog",
                    Qgis.Info
                )
                return query
            
            table_names_str = from_match.group(1)
            
            # Extract individual table names (handle aliases)
            table_pattern = r'([^\s,]+(?:\.[^\s,]+)*)'
            table_matches = re.findall(table_pattern, table_names_str)
            
            if not table_matches:
                return query
            
            # Get geometry columns for the tables
            geometry_columns = self._get_geometry_columns_for_tables(connection, table_matches)
            
            if not geometry_columns:
                QgsMessageLog.logMessage(
                    "No geometry columns found in queried tables",
                    "Query Dialog",
                    Qgis.Info
                )
                return query
            
            # Modify the SELECT clause to add ST_ASWKT for geometry columns
            modified_query = self._modify_select_clause(query, geometry_columns)
            
            if modified_query != query:
                QgsMessageLog.logMessage(
                    f"Modified query to add ST_ASWKT conversion:\nOriginal: {query}\nModified: {modified_query}",
                    "Query Dialog",
                    Qgis.Info
                )
            
            return modified_query
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error analyzing query for geometry conversion: {str(e)}, using original query",
                "Query Dialog",
                Qgis.Warning
            )
            return query
    
    def _get_geometry_columns_for_tables(self, connection, table_names):
        """Get geometry columns for the specified tables"""
        geometry_columns = {}
        
        try:
            with connection.cursor() as cursor:
                for table_name in table_names:
                    # Handle fully qualified names (catalog.schema.table)
                    parts = table_name.split('.')
                    if len(parts) == 3:
                        catalog, schema, table = parts
                    elif len(parts) == 2:
                        catalog, schema, table = None, parts[0], parts[1]
                    else:
                        catalog, schema, table = None, None, parts[0]
                    
                    # Query information_schema for geometry columns
                    where_conditions = [f"table_name = '{table}'"]
                    if schema:
                        where_conditions.append(f"table_schema = '{schema}'")
                    if catalog:
                        where_conditions.append(f"table_catalog = '{catalog}'")
                    
                    where_clause = " AND ".join(where_conditions)
                    
                    info_query = f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns 
                        WHERE {where_clause}
                        AND data_type IN ('GEOGRAPHY', 'GEOMETRY')
                    """
                    
                    QgsMessageLog.logMessage(
                        f"Checking for geometry columns in {table_name}: {info_query}",
                        "Query Dialog",
                        Qgis.Info
                    )
                    
                    cursor.execute(info_query)
                    results = cursor.fetchall()
                    
                    for row in results:
                        column_name = row[0]
                        data_type = row[1]
                        
                        # Store both simple column name and qualified names
                        geometry_columns[column_name] = data_type
                        geometry_columns[f"{table_name}.{column_name}"] = data_type
                        if table:
                            geometry_columns[f"{table}.{column_name}"] = data_type
                        
                        QgsMessageLog.logMessage(
                            f"Found geometry column: {column_name} ({data_type}) in table {table_name}",
                            "Query Dialog",
                            Qgis.Info
                        )
        
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error getting geometry columns: {str(e)}",
                "Query Dialog",
                Qgis.Warning
            )
        
        return geometry_columns
    
    def _modify_select_clause(self, query, geometry_columns):
        """Modify the SELECT clause to add ST_ASWKT for geometry columns"""
        try:
            import re
            
            # Find the SELECT clause
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
            if not select_match:
                return query
            
            select_clause = select_match.group(1)
            
            # Handle SELECT * case
            if select_clause.strip() == '*':
                # For SELECT *, we can't easily modify without knowing all columns
                # Return original query and let user handle geometry conversion manually
                QgsMessageLog.logMessage(
                    "SELECT * detected - cannot automatically add ST_ASWKT. Use explicit column names for automatic conversion.",
                    "Query Dialog",
                    Qgis.Info
                )
                return query
            
            # Split select items and process each one
            select_items = []
            current_item = ""
            paren_level = 0
            
            for char in select_clause:
                if char == '(':
                    paren_level += 1
                elif char == ')':
                    paren_level -= 1
                elif char == ',' and paren_level == 0:
                    select_items.append(current_item.strip())
                    current_item = ""
                    continue
                current_item += char
            
            if current_item.strip():
                select_items.append(current_item.strip())
            
            # Process each select item
            modified_items = []
            for item in select_items:
                modified_item = self._process_select_item(item, geometry_columns)
                modified_items.append(modified_item)
            
            # Rebuild the query
            modified_select_clause = ', '.join(modified_items)
            modified_query = query.replace(select_match.group(0), f"SELECT {modified_select_clause} FROM", 1)
            
            return modified_query
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error modifying SELECT clause: {str(e)}",
                "Query Dialog",
                Qgis.Warning
            )
            return query
    
    def _process_select_item(self, item, geometry_columns):
        """Process a single SELECT item to add ST_ASWKT if it's a geometry column"""
        import re
        
        item = item.strip()
        
        # Check if this item is a geometry column (handle aliases)
        base_column = item
        alias = None
        
        # Check for AS alias
        as_match = re.search(r'^(.+?)\s+AS\s+(.+)$', item, re.IGNORECASE)
        if as_match:
            base_column = as_match.group(1).strip()
            alias = as_match.group(2).strip()
        else:
            # Check for space alias (without AS)
            space_match = re.search(r'^(.+?)\s+([^\s]+)$', item)
            if space_match and not any(op in item.upper() for op in ['(', ')', '+', '-', '*', '/', 'CASE', 'WHEN']):
                base_column = space_match.group(1).strip()
                alias = space_match.group(2).strip()
        
        # Remove quotes if present
        clean_column = base_column.strip('"').strip("'").strip('`')
        
        # Check if this is a geometry column
        if clean_column in geometry_columns:
            QgsMessageLog.logMessage(
                f"Converting geometry column {clean_column} to WKT format",
                "Query Dialog",
                Qgis.Info
            )
            
            # Wrap with ST_ASWKT
            converted = f"ST_ASWKT({base_column})"
            
            # Preserve alias
            if alias:
                return f"{converted} AS {alias}"
            else:
                return converted
        
        return item


class DatabricksQueryDialog(QDialog):
    """Dialog for executing custom SQL queries against Databricks"""
    
    def __init__(self, connection_config, parent=None, initial_query=""):
        super().__init__(parent)
        self.connection_config = connection_config
        self.setWindowTitle("Databricks Custom Query")
        self.setModal(True)
        self.resize(1000, 700)
        
        self.setup_ui()
        
        if initial_query:
            self.query_edit.setPlainText(initial_query)
        
        # Auto-refresh database structure on first load
        if self.connection_config:
            self.db_loading_label.setText("Auto-loading database structure...")
            self.refresh_database_structure()
    
    def setup_ui(self):
        """Setup the user interface"""
        layout = QVBoxLayout(self)
        
        # Create horizontal splitter for database browser and query/results
        main_splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(main_splitter)
        
        # Left side: Database browser
        self.setup_database_browser(main_splitter)
        
        # Right side: Query and results
        right_widget = self.setup_query_results_area()
        main_splitter.addWidget(right_widget)
        
        # Set splitter proportions: 30% database browser, 70% query/results
        main_splitter.setSizes([300, 700])
    
    def setup_database_browser(self, parent):
        """Setup the database structure browser"""
        db_group = QGroupBox("Database Structure")
        db_layout = QVBoxLayout(db_group)
        
        # Refresh button
        refresh_layout = QHBoxLayout()
        self.refresh_db_btn = QPushButton("Refresh")
        self.refresh_db_btn.clicked.connect(self.refresh_database_structure)
        refresh_layout.addWidget(self.refresh_db_btn)
        refresh_layout.addStretch()
        db_layout.addLayout(refresh_layout)
        
        # Database tree
        self.db_tree = QTreeWidget()
        self.db_tree.setHeaderLabel("Catalogs / Schemas / Tables")
        self.db_tree.itemDoubleClicked.connect(self.on_tree_item_double_clicked)
        db_layout.addWidget(self.db_tree)
        
        # Loading label
        self.db_loading_label = QLabel("Database structure will auto-load on dialog open")
        self.db_loading_label.setStyleSheet("color: gray; font-style: italic;")
        db_layout.addWidget(self.db_loading_label)
        
        parent.addWidget(db_group)
        
        # Store database structure
        self.database_structure = {}
    
    def setup_query_results_area(self):
        """Setup the query and results area"""
        # Create splitter for query and results
        splitter = QSplitter(Qt.Vertical)
        
        return self.setup_query_and_results(splitter)
    
    def setup_query_and_results(self, splitter):
        # Query input section
        query_group = QGroupBox("SQL Query")
        query_layout = QVBoxLayout(query_group)
        
        # Query text area
        self.query_edit = QTextEdit()
        self.query_edit.setPlaceholderText(
            "Enter your SQL query here...\n\n"
            "Examples:\n"
            "SELECT * FROM catalog.schema.table LIMIT 100\n"
            "SELECT id, name, geometry FROM catalog.schema.spatial_table LIMIT 100\n"
            "SELECT id, name, geometry FROM catalog.schema.spatial_table WHERE ST_INTERSECTS(geometry, ST_GEOMFROMTEXT('POINT(0 0)'))\n"
            "SHOW TABLES IN catalog.schema\n\n"
            "✨ Tip: Geometry columns (GEOMETRY/GEOGRAPHY types) are automatically converted to WKT format for QGIS compatibility!\n"
            "💡 Double-click items in the Database Structure to insert them into your query."
        )
        self.query_edit.setMinimumHeight(150)
        query_layout.addWidget(self.query_edit)
        
        # Query controls
        query_controls = QHBoxLayout()
        
        self.execute_btn = QPushButton("Execute Query")
        self.execute_btn.clicked.connect(self.execute_query)
        query_controls.addWidget(self.execute_btn)
        
        self.clear_btn = QPushButton("Clear")
        self.clear_btn.clicked.connect(self.clear_query)
        query_controls.addWidget(self.clear_btn)
        
        query_controls.addStretch()
        
        # Add as layer controls
        self.layer_name_edit = QLineEdit()
        self.layer_name_edit.setPlaceholderText("Layer name (optional)")
        query_controls.addWidget(QLabel("Layer name:"))
        query_controls.addWidget(self.layer_name_edit)
        
        self.geometry_column_edit = QLineEdit()
        self.geometry_column_edit.setPlaceholderText("Geometry column (auto-detect)")
        query_controls.addWidget(QLabel("Geometry column:"))
        query_controls.addWidget(self.geometry_column_edit)
        
        self.add_layer_btn = QPushButton("Add as Layer")
        self.add_layer_btn.clicked.connect(self.add_as_layer)
        self.add_layer_btn.setEnabled(False)
        query_controls.addWidget(self.add_layer_btn)
        
        query_layout.addLayout(query_controls)
        
        splitter.addWidget(query_group)
        
        # Results section
        results_group = QGroupBox("Query Results")
        results_layout = QVBoxLayout(results_group)
        
        # Results table
        self.results_table = QTableWidget()
        self.results_table.setAlternatingRowColors(True)
        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        results_layout.addWidget(self.results_table)
        
        # Results info
        self.results_info = QLabel("No query executed")
        results_layout.addWidget(self.results_info)
        
        splitter.addWidget(results_group)
        
        # Set splitter proportions for query/results
        splitter.setSizes([300, 400])
        
        # Create a container widget for the splitter and button
        container = QWidget()
        container_layout = QVBoxLayout(container)
        container_layout.addWidget(splitter)
        
        # Button box
        button_layout = QHBoxLayout()
        
        self.close_btn = QPushButton("Close")
        self.close_btn.clicked.connect(self.close)
        button_layout.addStretch()
        button_layout.addWidget(self.close_btn)
        
        container_layout.addLayout(button_layout)
        
        # Store query results for layer creation
        self.last_query = ""
        self.last_columns = []
        self.last_rows = []
        
        return container
    
    def clear_query(self):
        """Clear the query text"""
        self.query_edit.clear()
    
    def execute_query(self):
        """Execute the SQL query"""
        if not DATABRICKS_AVAILABLE:
            QMessageBox.critical(self, "Missing Dependencies", 
                               "databricks-sql-connector is not installed.")
            return
        
        query = self.query_edit.toPlainText().strip()
        if not query:
            QMessageBox.warning(self, "No Query", "Please enter a SQL query.")
            return
        
        # Show progress dialog
        self.progress_dialog = QProgressDialog("Executing query...", "Cancel", 0, 0, self)
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.show()
        
        # Start query thread
        self.query_thread = QueryExecutionThread(self.connection_config, query)
        self.query_thread.progress.connect(self.on_query_progress)
        self.query_thread.finished.connect(self.on_query_finished)
        self.query_thread.start()
    
    def on_query_progress(self, message):
        """Update progress dialog"""
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.setLabelText(message)
    
    def on_query_finished(self, success, message, columns, rows):
        """Handle query execution results"""
        self.progress_dialog.close()
        
        if success:
            self.display_results(columns, rows)
            self.results_info.setText(f"Query executed successfully. {len(rows)} rows returned.")
            
            # Store results for layer creation
            self.last_query = self.query_edit.toPlainText().strip()
            self.last_columns = columns
            self.last_rows = rows
            
            # Enable add layer button if we have results
            self.add_layer_btn.setEnabled(len(rows) > 0)
            
        else:
            self.results_table.clear()
            self.results_table.setRowCount(0)
            self.results_table.setColumnCount(0)
            self.results_info.setText(f"Query failed: {message}")
            self.add_layer_btn.setEnabled(False)
            
            QMessageBox.critical(self, "Query Error", message)
    
    def display_results(self, columns, rows):
        """Display query results in the table"""
        if not columns or not rows:
            self.results_table.clear()
            self.results_table.setRowCount(0)
            self.results_table.setColumnCount(0)
            return
        
        # Setup table
        self.results_table.setColumnCount(len(columns))
        self.results_table.setRowCount(len(rows))
        self.results_table.setHorizontalHeaderLabels(columns)
        
        # Populate table
        for i, row in enumerate(rows):
            for j, value in enumerate(row):
                # Convert value to string for display
                display_value = str(value) if value is not None else ""
                
                # Truncate very long values (like geometry WKT)
                if len(display_value) > 200:
                    display_value = display_value[:200] + "..."
                
                item = QTableWidgetItem(display_value)
                item.setToolTip(str(value) if value is not None else "")
                self.results_table.setItem(i, j, item)
        
        # Auto-resize columns but limit maximum width
        self.results_table.resizeColumnsToContents()
        for i in range(len(columns)):
            if self.results_table.columnWidth(i) > 300:
                self.results_table.setColumnWidth(i, 300)
    
    def add_as_layer(self):
        """Add query results as a QGIS layer"""
        if not self.last_rows:
            QMessageBox.warning(self, "No Results", "No query results to add as layer.")
            return
        
        layer_name = self.layer_name_edit.text().strip()
        if not layer_name:
            layer_name = "Databricks_Query_Layer"
        
        geometry_column = self.geometry_column_edit.text().strip() or None
        
        # Show progress dialog
        self.progress_dialog = QProgressDialog("Creating layer...", "Cancel", 0, 0, self)
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.show()
        
        # Start layer creation thread
        self.layer_thread = QueryLayerCreationThread(
            self.connection_config, 
            self.last_query, 
            layer_name, 
            geometry_column
        )
        self.layer_thread.progress.connect(self.on_layer_progress)
        self.layer_thread.finished.connect(self.on_layer_finished)
        self.layer_thread.start()
    
    def on_layer_progress(self, message):
        """Update progress dialog"""
        if hasattr(self, 'progress_dialog'):
            self.progress_dialog.setLabelText(message)
    
    def on_layer_finished(self, success, message, layer):
        """Handle layer creation results"""
        self.progress_dialog.close()
        
        if success and layer:
            # Add layer to QGIS project
            QgsProject.instance().addMapLayer(layer)
            
            QgsMessageLog.logMessage(
                f"Added query layer: {layer.name()} with {layer.featureCount()} features",
                "Query Dialog",
                Qgis.Info
            )
            
            QMessageBox.information(self, "Layer Added", 
                                  f"Layer '{layer.name()}' added successfully with {layer.featureCount()} features.")
        else:
            QMessageBox.critical(self, "Layer Creation Failed", message)
    
    def refresh_database_structure(self):
        """Refresh the database structure tree"""
        if not DATABRICKS_AVAILABLE:
            QMessageBox.critical(self, "Missing Dependencies", 
                               "databricks-sql-connector is not installed.")
            return
        
        # Clear existing tree
        self.db_tree.clear()
        self.database_structure = {}
        
        # Show loading state
        self.db_loading_label.setText("Loading accessible database structure (faster than SHOW commands)...")
        self.refresh_db_btn.setEnabled(False)
        
        # Start database structure loading thread
        self.db_thread = DatabaseStructureThread(self.connection_config)
        self.db_thread.progress.connect(self.on_db_loading_progress)
        self.db_thread.finished.connect(self.on_db_structure_loaded)
        self.db_thread.start()
    
    def on_db_loading_progress(self, message):
        """Update loading progress"""
        self.db_loading_label.setText(message)
    
    def on_db_structure_loaded(self, structure):
        """Handle loaded database structure"""
        self.database_structure = structure
        self.refresh_db_btn.setEnabled(True)
        
        if not structure:
            self.db_loading_label.setText("Failed to load database structure. Check connection.")
            return
        
        # Populate the tree
        self.populate_database_tree(structure)
        total_tables = sum(len(tables) for catalog in structure.values() for tables in catalog.values())
        self.db_loading_label.setText(f"Loaded {len(structure)} catalogs, {total_tables} accessible tables. Double-click items to insert into query.")
    
    def populate_database_tree(self, structure):
        """Populate the database tree widget"""
        self.db_tree.clear()
        
        for catalog_name, schemas in structure.items():
            catalog_item = QTreeWidgetItem(self.db_tree)
            catalog_item.setText(0, f"📁 {catalog_name}")
            catalog_item.setData(0, Qt.UserRole, {'type': 'catalog', 'name': catalog_name})
            
            for schema_name, tables in schemas.items():
                schema_item = QTreeWidgetItem(catalog_item)
                schema_item.setText(0, f"📂 {schema_name}")
                schema_item.setData(0, Qt.UserRole, {'type': 'schema', 'catalog': catalog_name, 'name': schema_name})
                
                for table_name, table_info in tables.items():
                    table_item = QTreeWidgetItem(schema_item)
                    
                    # Check if table has geometry columns
                    has_geometry = any(col.get('is_geometry', False) for col in table_info.get('columns', []))
                    table_icon = "🗺️" if has_geometry else "📋"
                    
                    table_item.setText(0, f"{table_icon} {table_name}")
                    table_item.setData(0, Qt.UserRole, {
                        'type': 'table', 
                        'catalog': catalog_name,
                        'schema': schema_name,
                        'name': table_name,
                        'full_name': table_info.get('full_name', f"{catalog_name}.{schema_name}.{table_name}"),
                        'columns': table_info.get('columns', [])
                    })
                    
                    # Add columns as children
                    for col_info in table_info.get('columns', []):
                        col_item = QTreeWidgetItem(table_item)
                        
                        col_icon = "🌍" if col_info.get('is_geometry', False) else "📝"
                        col_type = col_info.get('type', 'unknown')
                        
                        col_item.setText(0, f"{col_icon} {col_info['name']} ({col_type})")
                        col_item.setData(0, Qt.UserRole, {
                            'type': 'column',
                            'catalog': catalog_name,
                            'schema': schema_name,
                            'table': table_name,
                            'name': col_info['name'],
                            'data_type': col_type,
                            'is_geometry': col_info.get('is_geometry', False)
                        })
        
        # Expand first level (catalogs)
        self.db_tree.expandToDepth(0)
    
    def on_tree_item_double_clicked(self, item, column):
        """Handle double-click on tree item to insert into query"""
        data = item.data(0, Qt.UserRole)
        if not data:
            return
        
        cursor = self.query_edit.textCursor()
        
        if data['type'] == 'catalog':
            text_to_insert = data['name']
        elif data['type'] == 'schema':
            text_to_insert = f"{data['catalog']}.{data['name']}"
        elif data['type'] == 'table':
            text_to_insert = data['full_name']
        elif data['type'] == 'column':
            text_to_insert = data['name']
        else:
            return
        
        # Insert the text at cursor position
        cursor.insertText(text_to_insert)
        
        # Focus back to query editor
        self.query_edit.setFocus()
        
        QgsMessageLog.logMessage(
            f"Inserted '{text_to_insert}' into query",
            "Query Dialog",
            Qgis.Info
        )