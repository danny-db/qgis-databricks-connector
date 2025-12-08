# QGIS Databricks DBSQL Connector

A QGIS plugin that provides direct connectivity to Databricks SQL warehouses, allowing you to load and display geospatial data from Unity Catalog tables directly in QGIS.

- Walkthrough (Mac): https://www.youtube.com/watch?v=M5ZvVWpZnQY
- Windows installation: https://www.youtube.com/watch?v=zpyWuKZTePQ

## Features

- **Direct Databricks SQL Connection**: Connect directly to Databricks SQL warehouses using personal access tokens. Connect to serverless SQL for the best performance.
- **Spatial Data Support**: Full support for GEOGRAPHY and GEOMETRY data types 
- **Multiple Access Methods**: Load data via Dialog, Browser Panel, or Custom Query interface
- **Browser Panel Integration**: Browse catalogs, schemas, and tables directly in QGIS Browser with context menu actions
- **Custom SQL Query Support**: Execute any SQL query and add results as layers with automatic geometry detection
- **Table Discovery**: Automatically discover tables with spatial columns (GEOMETRY, GEOGRAPHY) in your Unity Catalog
- **Memory Layer Creation**: Load spatial data as QGIS memory layers with full attribute support
- **Connection Management**: Save and manage multiple Databricks connections with persistent settings
- **Mixed Geometry Handling**: Automatically creates separate layers for different geometry types in the same table
- **Configurable Layer Naming**: Set custom layer name prefix that applies across all loading methods
- **Flexible Feature Limits**: Load all records or limit to specific count (default: unlimited)

## Requirements

### QGIS Version
- Tested QGIS 3.42.1 and 3.44.1 on Mac and Windows
- It should work with other versions too.

### Python Dependencies
- `databricks-sql-connector>=3.5.0` (required - for Databricks SQL connectivity)
- `shapely>=2.0.0` (optional but recommended - for geometry operations)
- `pyproj>=3.6.0` (optional - for advanced coordinate system projections)

**Note**: QGIS includes built-in WKT parsing, so the plugin works without shapely, but having it installed can improve geometry handling.

### Databricks Requirements
- Databricks SQL Warehouse access
- Personal Access Token with appropriate permissions
- Unity Catalog tables with GEOGRAPHY or GEOMETRY columns

## Installation

### Option 1: Install from QGIS Plugin Manager (Recommended)

1. Open QGIS
2. Go to `Plugins → Manage and Install Plugins`
3. Search for "Databricks DBSQL Connector"
4. Click `Install Plugin`
5. After installation, run the dependency installer (see Dependencies section below)

### Option 2: Install from ZIP File

1. Download the plugin ZIP file from the [Releases](https://github.com/danny-db/qgis-databricks-connector/releases) page
2. In QGIS: `Plugins → Manage and Install Plugins → Install from ZIP`
3. Select the downloaded ZIP file and click `Install Plugin`
4. Run the dependency installer (see below)

### Option 3: Manual Installation (Development)

1. **Clone the repository** to a local directory
2. **Run the installation script**:
   ```bash
   # For macOS 
   python3 install_macos.py
   
   # For Windows
   python3 install_windows.py
   ```
3. **Restart QGIS**
4. **Enable the plugin**:
   - Go to `Plugins → Manage and Install Plugins`
   - Find "Databricks DBSQL Connector" in the installed plugins
   - Check the box to enable it

### Installing Dependencies

The plugin requires the `databricks-sql-connector` package. After installing the plugin:

1. Open the QGIS Python Console (`Plugins → Python Console`)
2. Run the following commands:
   ```python
   import subprocess
   import sys
   subprocess.check_call([sys.executable, "-m", "pip", "install", "databricks-sql-connector>=3.5.0"])
   ```
3. Restart QGIS

## Usage

### Setting Up a Connection

1. **Open the plugin** by clicking the Databricks icon in the toolbar or going to `Plugins → Databricks DBSQL Connector → Connect to Databricks SQL`

2. **Enter connection details**:
   - **Connection Name**: A friendly name for saving this connection
   - **Server Hostname**: Your Databricks workspace hostname (e.g., `your-workspace.cloud.databricks.com`)
   - **HTTP Path**: The SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/your-warehouse-id`)
   - **Access Token**: Your Databricks personal access token (starts with `dapi`)

3. **Test the connection** by clicking "Test Connection"

4. **Configure layer options**:
   - **Layer Name Prefix**: Custom prefix for layer names (default: "databricks_")
   - **Max Features**: Leave empty for unlimited, or enter a number to limit (e.g., 1000)

5. **Save the connection** to persist settings by clicking "Save Connection"

### Method 1: Loading via Dialog (Table Discovery)

1. **Discover tables** by clicking "Discover Tables" after a successful connection test

2. **Select tables** to load:
   - Check the boxes next to the tables you want to add to QGIS
   - Review the geometry column and type information

3. **Add layers** by clicking "Add Selected Layers"
   - Uses the configured layer prefix and max features settings
   - Automatically creates separate layers for mixed geometry types

### Method 2: Loading via Browser Panel

1. **Open the QGIS Browser Panel** (`View → Panels → Browser`)

2. **Navigate the hierarchy**:
   - Expand `Databricks` in the browser
   - Select your saved connection
   - Browse through Catalogs → Schemas → Tables

3. **Load data** by right-clicking on a spatial table:
   - **Add First 1000 Features**: Quick preview (default for double-click)
   - **Add All Features**: Load complete dataset without limit
   - **View Data...**: Preview data in custom query dialog

### Method 3: Custom SQL Queries

1. **Open Custom Query** from the connection dialog or browser context menu

2. **Browse database structure**:
   - Tree view shows all accessible catalogs, schemas, and tables
   - Double-click items to insert into query
   - 🗺️ icon indicates tables with geometry columns

3. **Write and execute your query**:
   ```sql
   SELECT id, name, geometry 
   FROM catalog.schema.spatial_table 
   WHERE condition
   ```
   - Geometry columns are automatically converted to WKT format
   - No need to use ST_ASWKT in your queries

4. **Add results as layer**:
   - Set Layer Name Prefix (uses saved setting by default)
   - Geometry column is auto-detected or can be specified
   - Click "Add as Layer" to create QGIS layer

### Working with the Data

- **Spatial data** is loaded as QGIS memory layers with full geometry support
- **Attributes** from all non-geometry columns are included
- **Mixed geometries** are automatically split into separate layers (e.g., Points, LineStrings, Polygons)
- **Identifier handling**: Tables/columns with hyphens or special characters are properly escaped
- **Styling** can be applied using standard QGIS symbology tools
- **Analysis** can be performed using QGIS spatial analysis tools

## Supported Geometry Types

The plugin has been tested with the following geometry types:
- **Point / MultiPoint**
- **LineString / MultiLineString**
- **Polygon / MultiPolygon**
- **Mixed Geometry Tables**: Automatically detected and split into separate layers

**Note**: Generic GEOMETRY columns are automatically analyzed to detect the actual geometry types present in the data.

## Troubleshooting

### Common Issues

#### "Connection failed"
- **Cause**: Invalid connection parameters or network issues
- **Solution**: 
  - Verify hostname, HTTP path, and access token
  - Check network connectivity to Databricks
  - Ensure the SQL warehouse is running.  Use serverless SQL for the best performance.

#### "No spatial tables found" or "Empty catalogs/schemas"
- **Cause**: No tables with GEOGRAPHY/GEOMETRY columns in accessible catalogs, or insufficient permissions
- **Solution**:
  - Verify you have access to the Unity Catalog
  - Check that tables have GEOMETRY or GEOGRAPHY data type columns
  - Ensure proper permissions on the catalogs, schemas, and tables
  - Use Custom Query to test direct SQL access: `SELECT * FROM catalog.schema.table LIMIT 10`
  - Check QGIS Log Messages for detailed error information

#### "Missing Dependencies"
- **Cause**: Required Python packages not installed
- **Solution**: Follow the dependency installation steps above

### Debug Logging

The plugin logs detailed information to the QGIS message log:
- Go to `View → Panels → Log Messages`
- Select "Databricks Connector" from the dropdown
- Review error messages and connection details

### Performance Tips

1. **Use feature limits for large tables**: 
   - Browser: Use "Add First 1000 Features" for quick preview
   - Dialog: Set Max Features to limit records (leave empty for unlimited)
   - Consider starting with smaller datasets to check geometry and attributes
2. **Use Serverless SQL** when possible for the best performance
3. **Use appropriate SQL warehouse sizes** for your data volumes
4. **Leverage custom queries** to filter data at the source using WHERE clauses
5. **Check the QGIS Log** (`View → Panels → Log Messages → Databricks Connector`) for detailed operation info

## Repository Structure

```
qgis-databricks-connector/
├── databricks_dbsql_connector/    # Plugin folder (for QGIS)
│   ├── __init__.py                # Plugin entry point
│   ├── metadata.txt               # Plugin metadata
│   ├── LICENSE                    # MIT License
│   ├── requirements.txt           # Python dependencies
│   ├── databricks_connector.py    # Main plugin class
│   ├── databricks_dialog.py       # Connection dialog and query UI
│   ├── databricks_browser.py      # Browser panel integration
│   ├── databricks_provider.py     # Data provider
│   └── icons/                     # Plugin icons
├── install_macos.py               # macOS installation script
├── install_windows.py             # Windows installation script
├── package_plugin.py              # Script to create plugin ZIP
├── README.md                      # This file
└── LICENSE                        # MIT License
```

## Packaging for Distribution

To create a ZIP file for upload to the QGIS Plugin Repository:

```bash
python3 package_plugin.py
```

This creates `databricks_dbsql_connector.zip` ready for upload to https://plugins.qgis.org

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

