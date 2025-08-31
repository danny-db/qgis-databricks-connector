# QGIS Databricks DBSQL Connector

A QGIS plugin that provides direct connectivity to Databricks SQL warehouses, allowing you to load and display geospatial data from Unity Catalog tables directly in QGIS.

## Features

- **Direct Databricks SQL Connection**: Connect directly to Databricks SQL warehouses using personal access tokens
- **Spatial Data Support**: Full support for GEOGRAPHY and GEOMETRY data types with automatic WKT/WKB conversion
- **Table Discovery**: Automatically discover tables with spatial columns in your Unity Catalog
- **Memory Layer Creation**: Load spatial data as QGIS memory layers with full attribute support
- **Connection Management**: Save and manage multiple Databricks connections
- **Geometry Type Detection**: Automatic detection of Point, LineString, Polygon, and Multi-geometry types
- **CRS Support**: Proper coordinate reference system handling (EPSG:4326 default)

## Requirements

### QGIS Version
- QGIS 3.16 or higher

### Python Dependencies
- `databricks-sql-connector>=3.5.0`
- `shapely>=2.0.0`
- `pyproj>=3.6.0` (optional, for advanced projections)

### Databricks Requirements
- Databricks SQL Warehouse access
- Personal Access Token with appropriate permissions
- Unity Catalog tables with GEOGRAPHY or GEOMETRY columns

## Installation

### Automatic Installation (Recommended)

1. **Download the plugin files** to a local directory
2. **Run the installation script**:
   ```bash
   # For macOS
   python3 install_macos.py
   
   # For Windows/Linux
   python3 install.py
   ```
3. **Restart QGIS**
4. **Enable the plugin**:
   - Go to `Plugins → Manage and Install Plugins`
   - Find "Databricks DBSQL Connector" in the installed plugins
   - Check the box to enable it

### Manual Installation

1. **Install Python dependencies**:
   - Open QGIS Python Console (`Plugins → Python Console`)
   - Run the following commands:
   ```python
   import subprocess
   import sys
   subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'databricks-sql-connector'])
   subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'shapely'])
   ```

2. **Copy plugin files**:
   - Locate your QGIS plugins directory:
     - **Windows**: `%APPDATA%/QGIS/QGIS3/profiles/default/python/plugins/`
     - **macOS**: `~/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins/`
     - **Linux**: `~/.local/share/QGIS/QGIS3/profiles/default/python/plugins/`
   - Create a new folder called `databricks_connector`
   - Copy all plugin files to this folder

3. **Restart QGIS and enable the plugin**

## Usage

### Setting Up a Connection

1. **Open the plugin** by clicking the Databricks icon in the toolbar or going to `Plugins → Databricks DBSQL Connector → Connect to Databricks SQL`

2. **Enter connection details**:
   - **Connection Name**: A friendly name for saving this connection
   - **Server Hostname**: Your Databricks workspace hostname (e.g., `your-workspace.cloud.databricks.com`)
   - **HTTP Path**: The SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/your-warehouse-id`)
   - **Access Token**: Your Databricks personal access token (starts with `dapi`)

3. **Test the connection** by clicking "Test Connection"

4. **Save the connection** for future use by clicking "Save Connection"

### Loading Spatial Data

1. **Discover tables** by clicking "Discover Tables" after a successful connection test

2. **Select tables** to load:
   - Check the boxes next to the tables you want to add to QGIS
   - Review the geometry column and type information

3. **Configure layer options**:
   - **Layer Name Prefix**: Prefix for layer names in QGIS (default: "databricks_")
   - **Max Features**: Maximum number of features to load (default: 1000)

4. **Add layers** by clicking "Add Selected Layers"

### Working with the Data

- **Spatial data** is loaded as QGIS memory layers with full geometry support
- **Attributes** from all non-geometry columns are included
- **Styling** can be applied using standard QGIS symbology tools
- **Analysis** can be performed using QGIS spatial analysis tools

## Configuration

### Connection String Format
The plugin uses Databricks SQL connector with the following connection parameters:
```
databricks://hostname:port/http_path?access_token=token&table=schema.table&geom_column=geometry
```

### Supported Data Types

| Databricks Type | QGIS Type | Notes |
|----------------|-----------|-------|
| GEOGRAPHY | Geometry | Converted via WKT |
| GEOMETRY | Geometry | Converted via WKT |
| STRING | String | |
| INT | Integer | |
| BIGINT | Long Long | |
| DOUBLE | Double | |
| BOOLEAN | Boolean | |
| TIMESTAMP | DateTime | |
| DATE | Date | |

### Geometry Types Supported
- Point / MultiPoint
- LineString / MultiLineString  
- Polygon / MultiPolygon
- Generic Geometry (auto-detected)

## Troubleshooting

### Common Issues

#### "Failed to add features to layer"
- **Cause**: Mismatch between feature schema and layer schema
- **Solution**: This has been fixed in the latest version. Ensure you're using the updated code.

#### "Connection failed"
- **Cause**: Invalid connection parameters or network issues
- **Solution**: 
  - Verify hostname, HTTP path, and access token
  - Check network connectivity to Databricks
  - Ensure the SQL warehouse is running

#### "No spatial tables found"
- **Cause**: No tables with GEOGRAPHY/GEOMETRY columns in accessible catalogs
- **Solution**:
  - Verify you have access to the Unity Catalog
  - Check that tables have spatial columns
  - Ensure proper permissions on the tables

#### "Missing Dependencies"
- **Cause**: Required Python packages not installed
- **Solution**: Follow the dependency installation steps above

### Debug Logging

The plugin logs detailed information to the QGIS message log:
- Go to `View → Panels → Log Messages`
- Select "Databricks Connector" from the dropdown
- Review error messages and connection details

### Performance Tips

1. **Use Max Features limit** to prevent loading very large datasets
2. **Apply spatial filters** when possible in your Databricks tables
3. **Index geometry columns** in Databricks for better performance
4. **Use appropriate SQL warehouse sizes** for your data volumes

## Development

### Plugin Structure
```
databricks_connector/
├── __init__.py                 # Plugin entry point
├── databricks_connector.py     # Main plugin class
├── databricks_provider.py      # Data provider implementation  
├── databricks_dialog.py        # User interface
├── metadata.txt               # Plugin metadata
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

### Key Classes
- `DatabricksConnector`: Main plugin class
- `DatabricksProvider`: QGIS data provider for direct integration
- `DatabricksDialog`: Connection and table selection UI
- `LayerLoadingThread`: Async data loading with progress

## License

This plugin is distributed under an open source license. Please refer to the license file for details.

## Support

For issues, questions, or feature requests, please:
1. Check the troubleshooting section above
2. Review the QGIS message logs for error details
3. Contact the plugin author or submit issues to the project repository

## Version History

### v1.0.0
- Initial release
- Direct Databricks SQL connectivity
- Support for GEOGRAPHY and GEOMETRY data types
- Table discovery and connection management
- Memory layer creation with full attribute support
