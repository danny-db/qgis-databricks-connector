"""
Databricks Live Layer Manager - Auto-refreshing layers based on viewport extent

This module provides functionality for creating "live" layers that automatically
refresh their data from Databricks when the map viewport changes.
"""

import os
from typing import Optional, Dict, Any, List
from qgis.PyQt.QtCore import (
    QObject, QTimer, QThread, pyqtSignal, QSettings,
    QDate, QTime, QDateTime, QVariant
)
from qgis.PyQt.QtWidgets import QProgressBar, QApplication
from qgis.core import (
    QgsMessageLog, Qgis, QgsProject, QgsVectorLayer,
    QgsFeature, QgsGeometry, QgsFields, QgsField,
    QgsRectangle, QgsCoordinateReferenceSystem,
    QgsCoordinateTransform, QgsWkbTypes
)

# Check if databricks is available
try:
    from databricks import sql
    DATABRICKS_AVAILABLE = True
except ImportError:
    DATABRICKS_AVAILABLE = False


class LiveLayerFetchThread(QThread):
    """Background thread for fetching data from Databricks with viewport filtering"""
    
    progress = pyqtSignal(str)  # Progress message
    finished = pyqtSignal(bool, str, list)  # success, message, features_data
    
    def __init__(self, connection_config: Dict[str, str], table_info: Dict[str, Any],
                 extent: QgsRectangle, fields: QgsFields, max_features: int = 10000,
                 srid: int = 4326):
        super().__init__()
        self.connection_config = connection_config
        self.table_info = table_info
        self.extent = extent
        self.fields = fields
        self.max_features = max_features
        self.srid = srid
        self._cancelled = False
    
    def cancel(self):
        """Cancel the fetch operation"""
        self._cancelled = True
    
    def _escape_identifier(self, identifier: str) -> str:
        """Escape identifier with backticks for Databricks SQL"""
        if not identifier:
            return identifier
        identifier = identifier.strip('`')
        return f"`{identifier}`"
    
    def _get_escaped_table_ref(self) -> str:
        """Get properly escaped table reference"""
        full_name = self.table_info.get('full_name', '')
        parts = full_name.split('.')
        escaped_parts = [self._escape_identifier(part) for part in parts]
        return '.'.join(escaped_parts)
    
    def _build_viewport_query(self) -> str:
        """Build SQL query with viewport bounding box filter"""
        table_ref = self._get_escaped_table_ref()
        geometry_column = self.table_info.get('geometry_column', 'geometry')
        escaped_geom_col = self._escape_identifier(geometry_column)
        
        # Build attribute columns list
        attr_columns = [self._escape_identifier(f.name()) for f in self.fields]
        attr_sql = ", ".join(attr_columns) if attr_columns else "1"
        
        # Build viewport polygon WKT
        xmin, ymin = self.extent.xMinimum(), self.extent.yMinimum()
        xmax, ymax = self.extent.xMaximum(), self.extent.yMaximum()
        
        viewport_wkt = (
            f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, "
            f"{xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))"
        )
        
        # Build query with ST_INTERSECTS spatial filter
        # Use the layer's SRID for the viewport geometry
        # Use ST_ASWKB for more efficient binary transfer (returns hex-encoded WKB)
        query = f"""
            SELECT {attr_sql}, ST_ASWKB({escaped_geom_col}) as geometry_wkb
            FROM {table_ref}
            WHERE ST_INTERSECTS(
                {escaped_geom_col},
                ST_GEOMFROMTEXT('{viewport_wkt}', {self.srid})
            )
        """
        
        # Add custom WHERE clause if specified
        custom_where = self.table_info.get('custom_where', '')
        if custom_where:
            query += f" AND ({custom_where})"
        
        # Add LIMIT clause
        if self.max_features > 0:
            query += f" LIMIT {self.max_features}"
        
        return query
    
    def run(self):
        """Execute the data fetch"""
        if not DATABRICKS_AVAILABLE:
            self.finished.emit(False, "databricks-sql-connector not installed", [])
            return
        
        if self._cancelled:
            self.finished.emit(False, "Fetch cancelled", [])
            return
        
        try:
            self.progress.emit("Connecting to Databricks...")
            
            connection = sql.connect(
                server_hostname=self.connection_config['hostname'],
                http_path=self.connection_config['http_path'],
                access_token=self.connection_config['access_token']
            )
            
            if self._cancelled:
                connection.close()
                self.finished.emit(False, "Fetch cancelled", [])
                return
            
            self.progress.emit("Fetching data for viewport...")
            
            query = self._build_viewport_query()
            
            QgsMessageLog.logMessage(
                f"Live layer query: {query[:500]}...",
                "Databricks Live Layer",
                Qgis.Info
            )
            
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
            
            connection.close()
            
            if self._cancelled:
                self.finished.emit(False, "Fetch cancelled", [])
                return
            
            self.progress.emit(f"Processing {len(rows)} features...")
            
            # Return raw data for processing on main thread
            self.finished.emit(True, f"Fetched {len(rows)} features", list(rows))
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error fetching live layer data: {str(e)}",
                "Databricks Live Layer",
                Qgis.Critical
            )
            self.finished.emit(False, f"Error: {str(e)}", [])


class DatabricksLiveLayerManager(QObject):
    """
    Manages a "live" Databricks layer that auto-refreshes based on viewport extent.
    
    This class wraps a QGIS memory layer and automatically refreshes its data
    from Databricks when the map canvas extent changes.
    """
    
    # Signal emitted when refresh starts/completes
    refreshStarted = pyqtSignal()
    refreshCompleted = pyqtSignal(int)  # feature count
    refreshFailed = pyqtSignal(str)  # error message
    
    def __init__(self, iface, layer: QgsVectorLayer, connection_config: Dict[str, str],
                 table_info: Dict[str, Any], refresh_delay_ms: int = 500,
                 buffer_percent: float = 0.1, max_features: int = 10000):
        """
        Initialize the live layer manager.
        
        Args:
            iface: QGIS interface
            layer: The memory layer to manage
            connection_config: Databricks connection parameters
            table_info: Table metadata (full_name, geometry_column, etc.)
            refresh_delay_ms: Debounce delay in milliseconds
            buffer_percent: Extent buffer as percentage (0.1 = 10%)
            max_features: Maximum features to fetch per refresh
        """
        super().__init__()
        
        self.iface = iface
        self.layer = layer
        self.connection_config = connection_config
        self.table_info = table_info
        self.refresh_delay_ms = refresh_delay_ms
        self.buffer_percent = buffer_percent
        self.max_features = max_features
        
        # State tracking
        self._enabled = True
        self._is_refreshing = False
        self._last_extent = None
        self._fetch_thread = None
        
        # Debounce timer
        self._debounce_timer = QTimer(self)
        self._debounce_timer.setSingleShot(True)
        self._debounce_timer.timeout.connect(self._do_refresh)
        
        # Connect to canvas extent changes
        self.iface.mapCanvas().extentsChanged.connect(self._on_extent_changed)
        
        # Connect to layer removal to clean up
        QgsProject.instance().layerWillBeRemoved.connect(self._on_layer_removed)
        
        # Store reference on layer for retrieval
        layer.setCustomProperty("databricks/live_layer_manager_id", id(self))
        
        QgsMessageLog.logMessage(
            f"Live layer manager created for '{layer.name()}'",
            "Databricks Live Layer",
            Qgis.Info
        )
    
    @property
    def enabled(self) -> bool:
        """Whether auto-refresh is enabled"""
        return self._enabled
    
    @enabled.setter
    def enabled(self, value: bool):
        """Enable or disable auto-refresh"""
        self._enabled = value
        if value:
            # Trigger immediate refresh when re-enabled
            self._on_extent_changed()
    
    def _on_extent_changed(self):
        """Handle map canvas extent change - starts debounce timer"""
        if not self._enabled:
            return
        
        # Don't refresh if layer is not visible
        if not self.layer or not self.layer.isValid():
            return
        
        # Restart debounce timer
        self._debounce_timer.start(self.refresh_delay_ms)
    
    def _on_layer_removed(self, layer_id: str):
        """Handle layer removal - clean up resources"""
        if self.layer and self.layer.id() == layer_id:
            self.cleanup()
    
    def _get_buffered_extent(self) -> QgsRectangle:
        """Get current canvas extent with buffer, transformed to EPSG:4326"""
        canvas = self.iface.mapCanvas()
        extent = canvas.extent()
        
        # Apply buffer
        if self.buffer_percent > 0:
            width_buffer = extent.width() * self.buffer_percent
            height_buffer = extent.height() * self.buffer_percent
            extent = QgsRectangle(
                extent.xMinimum() - width_buffer,
                extent.yMinimum() - height_buffer,
                extent.xMaximum() + width_buffer,
                extent.yMaximum() + height_buffer
            )
        
        # Transform to the layer's CRS if needed
        canvas_crs = canvas.mapSettings().destinationCrs()
        layer_crs = self.layer.crs()
        
        if canvas_crs != layer_crs:
            transform = QgsCoordinateTransform(
                canvas_crs, layer_crs, QgsProject.instance()
            )
            extent = transform.transformBoundingBox(extent)
        
        return extent
    
    def _is_extent_similar(self, extent: QgsRectangle) -> bool:
        """Check if new extent is similar enough to skip refresh"""
        if self._last_extent is None:
            return False
        
        # Skip if extent hasn't changed significantly (within 5%)
        tolerance = 0.05
        width_diff = abs(extent.width() - self._last_extent.width()) / max(extent.width(), 0.0001)
        height_diff = abs(extent.height() - self._last_extent.height()) / max(extent.height(), 0.0001)
        
        # Check if center has moved significantly
        center_diff_x = abs(extent.center().x() - self._last_extent.center().x())
        center_diff_y = abs(extent.center().y() - self._last_extent.center().y())
        center_moved = (center_diff_x > extent.width() * tolerance or 
                       center_diff_y > extent.height() * tolerance)
        
        size_changed = width_diff > tolerance or height_diff > tolerance
        
        return not (center_moved or size_changed)
    
    def _do_refresh(self):
        """Execute the data refresh"""
        if not self._enabled or self._is_refreshing:
            return
        
        if not self.layer or not self.layer.isValid():
            return
        
        extent = self._get_buffered_extent()
        
        # Skip if extent hasn't changed enough
        if self._is_extent_similar(extent):
            QgsMessageLog.logMessage(
                "Skipping refresh - extent unchanged",
                "Databricks Live Layer",
                Qgis.Info
            )
            return
        
        self._last_extent = extent
        self._is_refreshing = True
        self.refreshStarted.emit()
        
        # Cancel any existing fetch
        if self._fetch_thread and self._fetch_thread.isRunning():
            self._fetch_thread.cancel()
            self._fetch_thread.wait(1000)
        
        # Get the layer's SRID from its CRS
        layer_crs = self.layer.crs()
        srid = 4326  # Default fallback
        if layer_crs.isValid():
            # Try to get the EPSG code from the CRS
            auth_id = layer_crs.authid()  # Returns e.g. "EPSG:27700"
            if auth_id.startswith("EPSG:"):
                try:
                    srid = int(auth_id.split(":")[1])
                except (ValueError, IndexError):
                    pass
        
        # Start new fetch thread
        self._fetch_thread = LiveLayerFetchThread(
            self.connection_config,
            self.table_info,
            extent,
            self.layer.fields(),
            self.max_features,
            srid=srid
        )
        self._fetch_thread.finished.connect(self._on_fetch_finished)
        self._fetch_thread.start()
    
    def _on_fetch_finished(self, success: bool, message: str, rows: list):
        """Handle fetch completion"""
        self._is_refreshing = False
        
        if not success:
            self.refreshFailed.emit(message)
            QgsMessageLog.logMessage(
                f"Live layer refresh failed: {message}",
                "Databricks Live Layer",
                Qgis.Warning
            )
            return
        
        if not self.layer or not self.layer.isValid():
            return
        
        try:
            # Update layer features
            feature_count = self._update_layer_features(rows)
            self.refreshCompleted.emit(feature_count)
            
            QgsMessageLog.logMessage(
                f"Live layer refreshed with {feature_count} features",
                "Databricks Live Layer",
                Qgis.Info
            )
            
        except Exception as e:
            self.refreshFailed.emit(str(e))
            QgsMessageLog.logMessage(
                f"Error updating layer features: {str(e)}",
                "Databricks Live Layer",
                Qgis.Critical
            )
    
    def _parse_wkb_hex(self, wkb_hex) -> Optional[QgsGeometry]:
        """Parse WKB hex string to QgsGeometry.
        
        Databricks ST_ASWKB returns hex-encoded WKB (as bytes or string).
        """
        if wkb_hex is None:
            return None
        
        try:
            # Handle both bytes and string input
            if isinstance(wkb_hex, bytes):
                wkb_bytes = wkb_hex
            elif isinstance(wkb_hex, str):
                # Remove any whitespace and convert hex string to bytes
                wkb_hex = wkb_hex.strip()
                if not wkb_hex:
                    return None
                wkb_bytes = bytes.fromhex(wkb_hex)
            else:
                return None
            
            # Create geometry from WKB
            geom = QgsGeometry()
            geom.fromWkb(wkb_bytes)
            
            if geom.isNull() or geom.isEmpty():
                return None
            
            return geom
            
        except Exception as e:
            QgsMessageLog.logMessage(
                f"Error parsing WKB: {str(e)}",
                "Databricks Live Layer",
                Qgis.Warning
            )
            return None
    
    def _update_layer_features(self, rows: list) -> int:
        """Update the memory layer with new features"""
        # Freeze canvas during update to prevent flicker
        self.iface.mapCanvas().freeze(True)
        
        try:
            provider = self.layer.dataProvider()
            layer_fields = self.layer.fields()
            
            # Delete all existing features
            existing_ids = [f.id() for f in self.layer.getFeatures()]
            if existing_ids:
                provider.deleteFeatures(existing_ids)
            
            # Build new features
            features_to_add = []
            
            for row in rows:
                try:
                    feature = QgsFeature(layer_fields)
                    
                    # Process attributes (all columns except the last one which is geometry)
                    raw_attrs = list(row[:-1]) if len(row) > 1 else []
                    processed_attrs = self._process_attributes(raw_attrs, layer_fields)
                    feature.setAttributes(processed_attrs)
                    
                    # Process geometry (last column - now WKB hex)
                    geom_wkb = row[-1] if row else None
                    geom = self._parse_wkb_hex(geom_wkb)
                    
                    if geom is not None:
                        # Convert to multi-type if layer expects it
                        layer_geom_type = self.layer.wkbType()
                        if QgsWkbTypes.isMultiType(layer_geom_type) and not geom.isMultipart():
                            geom.convertToMultiType()
                        
                        feature.setGeometry(geom)
                        features_to_add.append(feature)
                            
                except Exception as e:
                    QgsMessageLog.logMessage(
                        f"Error processing feature: {str(e)}",
                        "Databricks Live Layer",
                        Qgis.Warning
                    )
            
            # Add features to provider
            if features_to_add:
                provider.addFeatures(features_to_add)
            
            # Force the layer to recognize the changes
            self.layer.updateExtents()
            self.layer.triggerRepaint()
            
            return len(features_to_add)
            
        finally:
            self.iface.mapCanvas().freeze(False)
            self.iface.mapCanvas().refresh()
    
    def _process_attributes(self, raw_attrs: list, fields: QgsFields) -> list:
        """Process raw attribute values with proper type conversion"""
        processed = []
        
        for i, attr_value in enumerate(raw_attrs):
            if i >= len(fields):
                break
            
            field = fields[i]
            field_type = field.type()
            
            if attr_value is None:
                processed.append(None)
            elif field_type == QVariant.LongLong:
                processed.append(int(attr_value) if attr_value is not None else None)
            elif field_type == QVariant.Double:
                processed.append(float(attr_value) if attr_value is not None else None)
            elif field_type == QVariant.DateTime:
                if hasattr(attr_value, 'year'):
                    qdate = QDate(attr_value.year, attr_value.month, attr_value.day)
                    qtime = QTime(
                        attr_value.hour, attr_value.minute, attr_value.second,
                        attr_value.microsecond // 1000 if hasattr(attr_value, 'microsecond') else 0
                    )
                    processed.append(QDateTime(qdate, qtime))
                else:
                    processed.append(None)
            elif field_type == QVariant.Date:
                if hasattr(attr_value, 'year'):
                    processed.append(QDate(attr_value.year, attr_value.month, attr_value.day))
                else:
                    processed.append(None)
            elif field_type == QVariant.String:
                processed.append(str(attr_value) if attr_value is not None else None)
            else:
                processed.append(attr_value)
        
        return processed
    
    def force_refresh(self):
        """Force an immediate refresh regardless of extent changes"""
        self._last_extent = None  # Clear cached extent
        self._do_refresh()
    
    def set_custom_where(self, where_clause: str):
        """Set a custom WHERE clause to filter data"""
        self.table_info['custom_where'] = where_clause
        self.force_refresh()
    
    def cleanup(self):
        """Clean up resources"""
        # Disconnect signals
        try:
            self.iface.mapCanvas().extentsChanged.disconnect(self._on_extent_changed)
        except:
            pass
        
        try:
            QgsProject.instance().layerWillBeRemoved.disconnect(self._on_layer_removed)
        except:
            pass
        
        # Stop timer
        self._debounce_timer.stop()
        
        # Cancel any running fetch
        if self._fetch_thread and self._fetch_thread.isRunning():
            self._fetch_thread.cancel()
            self._fetch_thread.wait(2000)
        
        QgsMessageLog.logMessage(
            f"Live layer manager cleaned up",
            "Databricks Live Layer",
            Qgis.Info
        )


class LiveLayerRegistry:
    """
    Registry for tracking active live layer managers.
    
    This singleton class maintains references to all active live layer managers
    to prevent garbage collection and enable cleanup on plugin unload.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._managers = {}
        return cls._instance
    
    def register(self, layer_id: str, manager: DatabricksLiveLayerManager):
        """Register a live layer manager"""
        self._managers[layer_id] = manager
        QgsMessageLog.logMessage(
            f"Registered live layer manager for layer {layer_id}",
            "Databricks Live Layer",
            Qgis.Info
        )
    
    def unregister(self, layer_id: str):
        """Unregister and cleanup a live layer manager"""
        if layer_id in self._managers:
            self._managers[layer_id].cleanup()
            del self._managers[layer_id]
            QgsMessageLog.logMessage(
                f"Unregistered live layer manager for layer {layer_id}",
                "Databricks Live Layer",
                Qgis.Info
            )
    
    def get(self, layer_id: str) -> Optional[DatabricksLiveLayerManager]:
        """Get the manager for a layer"""
        return self._managers.get(layer_id)
    
    def cleanup_all(self):
        """Cleanup all managers (call on plugin unload)"""
        for layer_id in list(self._managers.keys()):
            self.unregister(layer_id)
    
    def get_all_managers(self) -> Dict[str, DatabricksLiveLayerManager]:
        """Get all active managers"""
        return dict(self._managers)


def create_live_layer(iface, layer: QgsVectorLayer, connection_config: Dict[str, str],
                     table_info: Dict[str, Any], **kwargs) -> DatabricksLiveLayerManager:
    """
    Create and register a live layer manager for the given layer.
    
    Args:
        iface: QGIS interface
        layer: The memory layer to manage
        connection_config: Databricks connection parameters
        table_info: Table metadata
        **kwargs: Additional options (refresh_delay_ms, buffer_percent, max_features)
    
    Returns:
        The created DatabricksLiveLayerManager instance
    """
    manager = DatabricksLiveLayerManager(
        iface, layer, connection_config, table_info, **kwargs
    )
    
    # Register in global registry
    LiveLayerRegistry().register(layer.id(), manager)
    
    # Store live mode flag on layer
    layer.setCustomProperty("databricks/is_live_layer", "true")
    
    return manager
