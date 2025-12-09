# QGIS Databricks DBSQL Connector

A QGIS plugin that provides direct connectivity to Databricks SQL warehouses, allowing you to load and display geospatial data from Unity Catalog tables directly in QGIS.

## 📥 Quick Install

**Download the latest release:** [databricks_dbsql_connector.zip](https://github.com/danny-db/qgis-databricks-connector/releases/latest/download/databricks_dbsql_connector.zip)

1. In QGIS: `Plugins → Manage and Install Plugins → Install from ZIP`
2. Select the downloaded `databricks_dbsql_connector.zip`
3. Click the **Databricks icon** → Accept automatic dependency installation
4. **Restart QGIS**

⚠️ **Important**: Download `databricks_dbsql_connector.zip` from the [Releases page](https://github.com/danny-db/qgis-databricks-connector/releases), NOT "Source code (zip)"!

---

## 📺 Video Tutorials

- [Walkthrough (Mac)](https://www.youtube.com/watch?v=M5ZvVWpZnQY)
- [Windows installation](https://www.youtube.com/watch?v=zpyWuKZTePQ)

## Features

- **Direct Databricks SQL Connection**: Connect directly to Databricks SQL warehouses using personal access tokens
- **Spatial Data Support**: Full support for GEOGRAPHY and GEOMETRY data types 
- **Multiple Access Methods**: Load data via Dialog, Browser Panel, or Custom Query interface
- **Browser Panel Integration**: Browse catalogs, schemas, and tables directly in QGIS Browser
- **Custom SQL Query Support**: Execute any SQL query and add results as layers
- **Table Discovery**: Automatically discover tables with spatial columns in your Unity Catalog
- **Memory Layer Creation**: Load spatial data as QGIS memory layers with full attribute support
- **Connection Management**: Save and manage multiple Databricks connections
- **Mixed Geometry Handling**: Automatically creates separate layers for different geometry types
- **Configurable Layer Naming**: Set custom layer name prefix
- **Flexible Feature Limits**: Load all records or limit to specific count

## Requirements

### QGIS Version
- Tested on QGIS 3.42.1, 3.44.1, and 3.44.5 (Mac and Windows)
- Should work with QGIS 3.16+

### Python Dependencies
- **`databricks-sql-connector`** - Required (installed automatically by the plugin)

> **Note**: `shapely` and `pyproj` are already bundled with QGIS - no additional installation needed.

### Databricks Requirements
- Databricks SQL Warehouse access (Serverless recommended for best performance)
- Personal Access Token with appropriate permissions
- Unity Catalog tables with GEOGRAPHY or GEOMETRY columns

## Installation

### Option 1: Install from ZIP File (Recommended) ⭐

1. **Download** [`databricks_dbsql_connector.zip`](https://github.com/danny-db/qgis-databricks-connector/releases/latest/download/databricks_dbsql_connector.zip) from the [Releases page](https://github.com/danny-db/qgis-databricks-connector/releases)

2. In QGIS: `Plugins → Manage and Install Plugins → Install from ZIP`

3. Select the downloaded ZIP file and click `Install Plugin`

4. Click the **Databricks icon** in the toolbar - the plugin will offer to install the required `databricks-sql-connector` package automatically

5. **Restart QGIS** after dependencies are installed

### Option 2: Install from QGIS Plugin Manager

*(Coming soon - after the plugin is published to the official repository)*

1. Go to `Plugins → Manage and Install Plugins`
2. Search for "Databricks DBSQL Connector"
3. Click `Install Plugin`

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
   - **View Data...**: Open custom query dialog

#### Method 3: Custom SQL Queries
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
│   ├── metadata.txt               # Plugin metadata
│   ├── LICENSE                    # MIT License
│   ├── databricks_connector.py    # Main plugin class
│   ├── databricks_dialog.py       # Connection dialog and query UI
│   ├── databricks_browser.py      # Browser panel integration
│   ├── databricks_provider.py     # Data provider
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
