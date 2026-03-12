# QGIS Databricks DBSQL Connector

A QGIS plugin that provides direct connectivity to Databricks SQL warehouses, allowing you to load and display geospatial data from Unity Catalog tables directly in QGIS.

## 📥 Quick Install

1. In QGIS: `Plugins → Manage and Install Plugins`
2. Search for **"Databricks DBSQL Connector"** → Click `Install Plugin`
3. Click the **Databricks icon** → Accept automatic dependency installation
4. **Restart QGIS**

Or install from ZIP: download [`databricks_dbsql_connector.zip`](https://github.com/danny-db/qgis-databricks-connector/releases/latest/download/databricks_dbsql_connector.zip) from the [Releases page](https://github.com/danny-db/qgis-databricks-connector/releases).

---

## 📺 Video Tutorials

- [Walkthrough (Mac)](https://www.youtube.com/watch?v=M5ZvVWpZnQY)
- [Windows installation](https://www.youtube.com/watch?v=zpyWuKZTePQ)

## What's New in v1.2.0

### Live Layers (Viewport-Based Auto-Refresh)
- **Live Layer mode**: Add spatial layers that automatically refresh as you pan and zoom the map
- **Viewport-aware queries**: Only fetches features within the current map extent via `ST_INTERSECTS`, enabling work with very large datasets
- **Mixed geometry support**: Automatically detects geometry types (`SELECT DISTINCT ST_GEOMETRYTYPE`) and creates separate live layers per type (Point, LineString, Polygon) — same behaviour as standard layer loading
- **Auto-centre on first load**: Map centres on the data automatically so the first refresh always returns results
- **Smart debounce**: Rapid panning consolidates into a single query (500ms debounce timer)
- **Extent similarity check**: Small pans (<5% change) are ignored to avoid unnecessary queries
- **Toggle live mode**: Enable/disable all live layers via Plugins menu or toolbar
- **Multi-layer support**: Run multiple live layers simultaneously from different tables

### QGIS 4 / Qt6 Compatibility
- **QGIS 4.0 support**: Full Qt6/PyQt6 compatibility while maintaining QGIS 3.16+ support
- **Qt6 compat shims**: Portable `_qt6_compat.py` module handles API differences automatically
- **Fixed**: Plugin load failures caused by removed Qt5 unscoped enums
- **Fixed**: Dependency installation on QGIS 4 macOS (switched to in-process pip)
- **Fixed**: Replaced deprecated `exec_()` with `exec()`
- **Fixed**: Hardcoded `PyQt5` import replaced with portable `qgis.PyQt` abstraction

## Features

- **Direct Databricks SQL Connection**: Connect directly to Databricks SQL warehouses using personal access tokens
- **Spatial Data Support**: Full support for GEOGRAPHY and GEOMETRY data types
- **Live Layers**: Viewport-based auto-refresh — layers update automatically as you pan and zoom
- **Multiple Access Methods**: Load data via Dialog, Browser Panel, or Custom Query interface
- **Browser Panel Integration**: Browse catalogs, schemas, and tables directly in QGIS Browser
- **Custom SQL Query Support**: Execute any SQL query and add results as layers
- **Table Discovery**: Automatically discover tables with spatial columns in your Unity Catalog
- **Memory Layer Creation**: Load spatial data as QGIS memory layers with full attribute support
- **Connection Management**: Save and manage multiple Databricks connections
- **Mixed Geometry Handling**: Automatically creates separate layers for different geometry types
- **Configurable Layer Naming**: Set custom layer name prefix
- **Flexible Feature Limits**: Load all records or limit to specific count
- **Layer Data Refresh**: Update existing layers with fresh data from Databricks
- **QGIS 3.x and 4.x Compatible**: Works across QGIS 3.16+ through QGIS 4.x (Qt5 and Qt6)

## Requirements

### QGIS Version
- Tested on QGIS 3.42.1, 3.44.1, 3.44.5 (Mac and Windows), and QGIS 4.0 (Qt6)
- Supports QGIS 3.16+ through 4.x

### Python Dependencies
- **`databricks-sql-connector`** - Required (installed automatically by the plugin)

> **Note**: `shapely` and `pyproj` are already bundled with QGIS - no additional installation needed.

### Databricks Requirements
- Databricks SQL Warehouse access (Serverless recommended for best performance)
- Personal Access Token with appropriate permissions
- Unity Catalog tables with GEOGRAPHY or GEOMETRY columns

## Installation

### Option 1: Install from QGIS Plugin Manager (Recommended) ⭐

1. Go to `Plugins → Manage and Install Plugins`
2. Search for **"Databricks DBSQL Connector"**
3. Click `Install Plugin`
4. Click the **Databricks icon** in the toolbar - the plugin will offer to install the required `databricks-sql-connector` package automatically
5. **Restart QGIS** after dependencies are installed

Plugin page: [plugins.qgis.org/plugins/databricks_dbsql_connector](https://plugins.qgis.org/plugins/databricks_dbsql_connector/)

### Option 2: Install from ZIP File

1. **Download** [`databricks_dbsql_connector.zip`](https://github.com/danny-db/qgis-databricks-connector/releases/latest/download/databricks_dbsql_connector.zip) from the [Releases page](https://github.com/danny-db/qgis-databricks-connector/releases)

2. In QGIS: `Plugins → Manage and Install Plugins → Install from ZIP`

3. Select the downloaded ZIP file and click `Install Plugin`

4. **Restart QGIS** after dependencies are installed

### Installing Dependencies Manually

If automatic dependency installation fails:

1. Open the QGIS Python Console (`Plugins → Python Console`)
2. Run:
   ```python
   import pip
   pip.main(['install', 'databricks-sql-connector'])
   ```
3. Restart QGIS

## Usage

### Setting Up a Connection

1. **Open the plugin** by clicking the Databricks icon in the toolbar

2. **Enter connection details**:
   - **Connection Name**: A friendly name for saving this connection
   - **Server Hostname**: Your Databricks workspace hostname (e.g., `your-workspace.cloud.databricks.com`)
   - **HTTP Path**: The SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/your-warehouse-id`)
   - **Access Token**: Your Databricks personal access token (starts with `dapi`)

3. **Test the connection** by clicking "Test Connection"

4. **Save the connection** to persist settings

### Loading Data

#### Method 1: Table Discovery (Dialog)
1. Click "Discover Tables" after connecting
2. Select tables to load
3. Click "Add Selected Layers"

#### Method 2: Browser Panel
1. Open Browser Panel (`View → Panels → Browser`)
2. Expand `Databricks` → Your Connection → Catalog → Schema
3. Right-click a table:
   - **Add First 1000 Features**: Quick preview
   - **Add All Features**: Load complete dataset
   - **Add as Live Layer (Viewport)**: Load with auto-refresh on pan/zoom
   - **View Data...**: Open custom query dialog

#### Method 3: Live Layers
1. Right-click a spatial table in Browser → **Add as Live Layer (Viewport)**
   - Or: Use the Dialog with "Live Mode" checkbox enabled
2. The map auto-centres on the data and begins loading features in the viewport
3. Pan and zoom the map — the layer auto-refreshes with features in the current extent
4. Tables with mixed geometry types automatically create separate live layers (e.g. Point + Polygon)
5. Toggle all live layers on/off via `Plugins → Databricks → Toggle Live Mode`

#### Method 4: Custom SQL Queries
1. Open Custom Query from the dialog or browser
2. Write your SQL query
3. Click "Execute Query"
4. Click "Add as Layer"

## Supported Geometry Types

- Point / MultiPoint
- LineString / MultiLineString
- Polygon / MultiPolygon
- Mixed Geometry Tables (automatically split into separate layers)

## Troubleshooting

### "Connection failed"
- Verify hostname, HTTP path, and access token
- Check network connectivity to Databricks
- Ensure the SQL warehouse is running

### "No spatial tables found"
- Verify Unity Catalog access permissions
- Check that tables have GEOMETRY or GEOGRAPHY columns
- Check QGIS Log Messages (`View → Panels → Log Messages → Databricks Connector`)

### Dependency Installation Issues
If automatic installation fails, try manual installation via Python Console (see above).

### Debug Logging
- Go to `View → Panels → Log Messages`
- Select "Databricks Connector" for detailed logs

## Repository Structure

```
qgis-databricks-connector/
├── databricks_dbsql_connector/    # Plugin folder
│   ├── __init__.py                # Plugin entry point
│   ├── _qt6_compat.py             # Qt5/Qt6 compatibility shims
│   ├── metadata.txt               # Plugin metadata
│   ├── LICENSE                    # MIT License
│   ├── databricks_connector.py    # Main plugin class
│   ├── databricks_dialog.py       # Connection dialog and query UI
│   ├── databricks_browser.py      # Browser panel integration
│   ├── databricks_provider.py     # Data provider
│   ├── databricks_live_layer.py   # Live layer viewport auto-refresh
│   └── icons/                     # Plugin icons
├── .github/workflows/             # GitHub Actions for releases
├── package_plugin.py              # Script to create plugin ZIP
├── README.md                      # This file
└── LICENSE                        # MIT License
```

## For Developers

### Creating a Release ZIP

```bash
python3 package_plugin.py
```

This creates `databricks_dbsql_connector.zip` for distribution.

### Creating a New Release

```bash
git tag v1.x.x
git push origin v1.x.x
```

GitHub Actions will automatically create a release with the plugin ZIP attached.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
