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
    QGroupBox, QTextEdit, QPlainTextEdit
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