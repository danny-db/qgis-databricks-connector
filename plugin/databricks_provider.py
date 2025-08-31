"""
Databricks Vector Data Provider implementation
"""
import json
from typing import List, Dict, Any, Optional, Tuple, Set
from qgis.PyQt.QtCore import QVariant
from qgis.core import (
    QgsVectorDataProvider,
    QgsAbstractFeatureSource, 
    QgsAbstractFeatureIterator,
    QgsFeatureRequest,
    QgsFeature,
    QgsFields,
    QgsField,
    QgsGeometry,
    QgsWkbTypes,
    QgsRectangle,
    QgsCoordinateReferenceSystem,
    QgsProviderMetadata,
    QgsDataProvider,
    QgsMessageLog,
    Qgis,
    NULL
)
import logging
from shapely import wkt, wkb
from shapely.geometry import Point, LineString, Polygon
from databricks import sql


class DatabricksFeatureIterator(QgsAbstractFeatureIterator):
    """Feature iterator for Databricks provider"""
    
    def __init__(self, source, request: QgsFeatureRequest):
        super().__init__(request)
        self.source = source
        self.features = []
        self.index = 0
        
        # Execute query and fetch features
        self._fetch_features()
    
    def _fetch_features(self):
        """Fetch features from Databricks based on the request"""
        try:
            features = self.source.get_features(self.mRequest)
            self.features = list(features)
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error fetching features: {str(e)}",
                "Databricks Provider",
                Qgis.Critical
            )
            self.features = []
    
    def fetchFeature(self, f: QgsFeature) -> bool:
        """Fetch next feature"""
        if self.index >= len(self.features):
            return False
        
        feature = self.features[self.index]
        f.setId(feature.id())
        f.setAttributes(feature.attributes())
        f.setGeometry(feature.geometry())
        f.setFields(feature.fields())
        
        self.index += 1
        return True
    
    def rewind(self) -> bool:
        """Rewind iterator to start"""
        self.index = 0
        return True
    
    def close(self) -> bool:
        """Close iterator"""
        return True


class DatabricksFeatureSource(QgsAbstractFeatureSource):
    """Feature source for Databricks provider"""
    
    def __init__(self, provider):
        super().__init__()
        self.provider = provider
    
    def getFeatures(self, request: QgsFeatureRequest):
        """Return feature iterator"""
        return DatabricksFeatureIterator(self, request)
    
    def get_features(self, request: QgsFeatureRequest):
        """Get features from Databricks"""
        return self.provider.get_features_impl(request)


class DatabricksProvider(QgsVectorDataProvider):
    """Databricks Vector Data Provider"""
    
    PROVIDER_KEY = 'databricks'
    PROVIDER_DESCRIPTION = 'Databricks SQL Data Provider'
    
    def __init__(self, uri: str = '', options=None, flags=None):
        super().__init__(uri, options or QgsDataProvider.ProviderOptions(), flags or Qgis.DataProviderReadFlags())
        
        # Parse URI components
        self._parse_uri(uri)
        
        # Connection and query details
        self.connection = None
        self.fields_cache = QgsFields()
        self.feature_count_cache = -1
        self.extent_cache = QgsRectangle()
        self.geometry_column = None
        self.geometry_type = QgsWkbTypes.Unknown
        
        # Initialize connection
        if self.is_valid_config():
            self._connect()
            self._initialize_layer()
    
    def _parse_uri(self, uri: str):
        """Parse the URI string to extract connection parameters"""
        # URI format: 
        # databricks://hostname:port/http_path?access_token=token&table=schema.table&geom_column=geometry
        
        self.hostname = ''
        self.port = 443
        self.http_path = ''
        self.access_token = ''
        self.table_name = ''
        self.schema_name = ''
        self.geometry_column_name = 'geometry'
        
        if not uri.startswith('databricks://'):
            return
        
        # Simple URI parsing (in production, use urllib.parse)
        try:
            # Remove protocol
            uri_parts = uri.replace('databricks://', '').split('?')
            base_part = uri_parts[0]
            params_part = uri_parts[1] if len(uri_parts) > 1 else ''
            
            # Parse base: hostname:port/http_path
            if '/' in base_part:
                host_port, self.http_path = base_part.split('/', 1)
                self.http_path = '/' + self.http_path
            else:
                host_port = base_part
            
            if ':' in host_port:
                self.hostname, port_str = host_port.split(':')
                self.port = int(port_str)
            else:
                self.hostname = host_port
            
            # Parse parameters
            if params_part:
                for param in params_part.split('&'):
                    if '=' in param:
                        key, value = param.split('=', 1)
                        if key == 'access_token':
                            self.access_token = value
                        elif key == 'table':
                            if '.' in value:
                                self.schema_name, self.table_name = value.split('.', 1)
                            else:
                                self.table_name = value
                        elif key == 'geom_column':
                            self.geometry_column_name = value
                            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error parsing URI: {str(e)}",
                "Databricks Provider",
                Qgis.Warning
            )
    
    def is_valid_config(self) -> bool:
        """Check if configuration is valid"""
        return bool(self.hostname and self.access_token and self.table_name)
    
    def _connect(self):
        """Establish connection to Databricks"""
        try:
            self.connection = sql.connect(
                server_hostname=self.hostname,
                http_path=self.http_path,
                access_token=self.access_token
            )
            QgsMessageLog.logMessage(
                "Connected to Databricks successfully",
                "Databricks Provider", 
                Qgis.Info
            )
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Failed to connect to Databricks: {str(e)}",
                "Databricks Provider",
                Qgis.Critical
            )
            self.connection = None
    
    def _initialize_layer(self):
        """Initialize layer by querying table schema and detecting geometry"""
        if not self.connection:
            return
            
        try:
            with self.connection.cursor() as cursor:
                # Get table schema
                table_ref = f"{self.schema_name}.{self.table_name}" if self.schema_name else self.table_name
                cursor.execute(f"DESCRIBE {table_ref}")
                schema_info = cursor.fetchall()
                
                # Build fields and detect geometry column
                self.fields_cache = QgsFields()
                
                for row in schema_info:
                    col_name = row[0]
                    col_type = row[1].upper()
                    
                    if col_type in ['GEOGRAPHY', 'GEOMETRY']:
                        if not self.geometry_column:
                            self.geometry_column = col_name
                            self._detect_geometry_type(table_ref, col_name)
                    else:
                        # Add as attribute field
                        qgs_type = self._map_databricks_type_to_qgs(col_type)
                        field = QgsField(col_name, qgs_type)
                        self.fields_cache.append(field)
                
                # Get feature count
                cursor.execute(f"SELECT COUNT(*) FROM {table_ref}")
                result = cursor.fetchone()
                self.feature_count_cache = result[0] if result else 0
                
                # Get spatial extent
                if self.geometry_column:
                    self._calculate_extent(table_ref)
                    
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error initializing layer: {str(e)}",
                "Databricks Provider",
                Qgis.Critical
            )
    
    def _detect_geometry_type(self, table_ref: str, geom_col: str):
        """Detect the geometry type of the layer"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT ST_GEOMETRYTYPE({geom_col}) as geom_type 
                    FROM {table_ref} 
                    WHERE {geom_col} IS NOT NULL 
                    LIMIT 1
                """)
                result = cursor.fetchone()
                
                if result and result[0]:
                    geom_type = result[0].upper()
                    if 'POINT' in geom_type:
                        self.geometry_type = QgsWkbTypes.Point
                    elif 'LINESTRING' in geom_type:
                        self.geometry_type = QgsWkbTypes.LineString
                    elif 'POLYGON' in geom_type:
                        self.geometry_type = QgsWkbTypes.Polygon
                    elif 'MULTIPOINT' in geom_type:
                        self.geometry_type = QgsWkbTypes.MultiPoint
                    elif 'MULTILINESTRING' in geom_type:
                        self.geometry_type = QgsWkbTypes.MultiLineString
                    elif 'MULTIPOLYGON' in geom_type:
                        self.geometry_type = QgsWkbTypes.MultiPolygon
                    else:
                        self.geometry_type = QgsWkbTypes.Unknown
                        
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error detecting geometry type: {str(e)}",
                "Databricks Provider",
                Qgis.Warning
            )
            self.geometry_type = QgsWkbTypes.Unknown
    
    def _calculate_extent(self, table_ref: str):
        """Calculate spatial extent of the layer"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    SELECT 
                        ST_XMIN(ST_ENVELOPE(ST_UNION({self.geometry_column}))) as min_x,
                        ST_YMIN(ST_ENVELOPE(ST_UNION({self.geometry_column}))) as min_y,
                        ST_XMAX(ST_ENVELOPE(ST_UNION({self.geometry_column}))) as max_x,
                        ST_YMAX(ST_ENVELOPE(ST_UNION({self.geometry_column}))) as max_y
                    FROM {table_ref}
                    WHERE {self.geometry_column} IS NOT NULL
                """)
                result = cursor.fetchone()
                
                if result and all(x is not None for x in result):
                    self.extent_cache = QgsRectangle(result[0], result[1], result[2], result[3])
                    
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error calculating extent: {str(e)}",
                "Databricks Provider",
                Qgis.Warning
            )
    
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
    
    # Required abstract methods implementation
    def featureSource(self):
        """Return feature source"""
        return DatabricksFeatureSource(self)
    
    def getFeatures(self, request: QgsFeatureRequest):
        """Return feature iterator"""
        return DatabricksFeatureIterator(DatabricksFeatureSource(self), request)
    
    def fields(self) -> QgsFields:
        """Return fields"""
        return self.fields_cache
    
    def featureCount(self) -> int:
        """Return feature count"""
        return self.feature_count_cache
    
    def wkbType(self) -> QgsWkbTypes.Type:
        """Return geometry type"""
        return self.geometry_type
    
    def extent(self) -> QgsRectangle:
        """Return layer extent"""
        return self.extent_cache
    
    def isValid(self) -> bool:
        """Check if provider is valid"""
        return self.connection is not None and self.is_valid_config()
    
    def name(self) -> str:
        """Return provider name"""
        return self.PROVIDER_KEY
    
    def description(self) -> str:
        """Return provider description"""
        return self.PROVIDER_DESCRIPTION
    
    def crs(self) -> QgsCoordinateReferenceSystem:
        """Return coordinate reference system"""
        return QgsCoordinateReferenceSystem("EPSG:4326")  # Assume WGS84
    
    def get_features_impl(self, request: QgsFeatureRequest):
        """Implementation of feature retrieval"""
        if not self.connection or not self.geometry_column:
            return
        
        try:
            with self.connection.cursor() as cursor:
                # Build query based on request
                table_ref = f"{self.schema_name}.{self.table_name}" if self.schema_name else self.table_name
                
                # Select fields
                field_names = [field.name() for field in self.fields_cache]
                if self.geometry_column:
                    field_names.append(f"ST_ASWKT({self.geometry_column}) as {self.geometry_column}")
                
                query = f"SELECT {', '.join(field_names)} FROM {table_ref}"
                
                # Add WHERE clause for spatial filter
                where_conditions = []
                if request.filterRect() and not request.filterRect().isEmpty():
                    rect = request.filterRect()
                    spatial_filter = f"""
                        ST_INTERSECTS({self.geometry_column}, 
                            ST_GEOMFROMTEXT('POLYGON(({rect.xMinimum()} {rect.yMinimum()}, {rect.xMaximum()} {rect.yMinimum()}, {rect.xMaximum()} {rect.yMaximum()}, {rect.xMinimum()} {rect.yMaximum()}, {rect.xMinimum()} {rect.yMinimum()}))', 4326))
                    """
                    where_conditions.append(spatial_filter)
                
                if where_conditions:
                    query += " WHERE " + " AND ".join(where_conditions)
                
                # Add LIMIT
                if request.limit() > 0:
                    query += f" LIMIT {request.limit()}"
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                # Convert to QgsFeature objects
                for i, row in enumerate(rows):
                    feature = QgsFeature(self.fields_cache)
                    feature.setId(i)
                    
                    # Set attributes (excluding geometry column)
                    attrs = []
                    geom_wkt = None
                    
                    for j, value in enumerate(row):
                        if j < len(self.fields_cache):
                            attrs.append(value)
                        else:
                            # This should be the geometry column (WKT)
                            geom_wkt = value
                    
                    feature.setAttributes(attrs)
                    
                    # Set geometry
                    if geom_wkt:
                        try:
                            shapely_geom = wkt.loads(geom_wkt)
                            qgs_geom = self._shapely_to_qgs_geometry(shapely_geom)
                            feature.setGeometry(qgs_geom)
                        except Exception as e:
                            QgsMessageLog.logMessage(
                                f"Error converting geometry: {str(e)}",
                                "Databricks Provider",
                                Qgis.Warning
                            )
                    
                    yield feature
                    
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error executing query: {str(e)}",
                "Databricks Provider",
                Qgis.Critical
            )
    
    def _shapely_to_qgs_geometry(self, shapely_geom):
        """Convert Shapely geometry to QgsGeometry"""
        wkt_str = shapely_geom.wkt
        return QgsGeometry.fromWkt(wkt_str)


class DatabricksProviderMetadata(QgsProviderMetadata):
    """Provider metadata for Databricks provider"""
    
    def __init__(self):
        super().__init__(
            DatabricksProvider.PROVIDER_KEY,
            DatabricksProvider.PROVIDER_DESCRIPTION
        )
    
    def createProvider(self, uri: str, options, flags):
        """Create provider instance"""
        return DatabricksProvider(uri, options, flags)
    
    def icon(self):
        """Return provider icon"""
        # Return default icon or load custom icon
        return super().icon()