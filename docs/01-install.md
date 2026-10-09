# 1. Install the plugin

[← User guide](README.md) · Next: [Connect to Databricks →](02-connect.md)

## Option A: from the QGIS Plugin Manager (recommended)
1. In QGIS, open **Plugins → Manage and Install Plugins…**
2. Search for **Databricks DBSQL Connector**.
3. Click **Install Plugin**.

## Option B: from a ZIP file
1. Download `databricks_dbsql_connector.zip` from the [latest release](https://github.com/danny-db/qgis-databricks-connector/releases/latest). Use the plugin ZIP, **not** the "Source code" ZIP.
2. In QGIS, open **Plugins → Manage and Install Plugins… → Install from ZIP**.
3. Choose the file and click **Install Plugin**.

## Install the Python dependency (first run only)
The plugin needs the `databricks-sql-connector` Python package.

1. The first time the plugin loads without it, QGIS asks: *"The Databricks SQL Connector package is required but not installed. Would you like to install it now?"*
2. Click **Yes**. The plugin installs it into your user Python packages.
3. **Restart QGIS** when it says so.

> **Corporate networks:** if the install fails with *"Connection refused"* to `pypi.org`, your network blocks the public Python package index. Ask IT for your organisation's PyPI mirror and add it to your pip config (`~/.config/pip/pip.conf` on macOS/Linux, `%APPDATA%\pip\pip.ini` on Windows):
> ```ini
> [global]
> index-url = https://<your-pypi-mirror>/simple
> ```
> Then restart QGIS and accept the install prompt again.

## Check it worked
- A **Databricks** icon (and a red chat-bubble **Genie One** icon) appears on the toolbar.
- **Plugins → Databricks DBSQL Connector** shows *Connect to Databricks SQL*, *Update Layer Data from Databricks*, *Toggle Live Mode for Layer*, *Databricks Genie Agent* and *Databricks Genie One*.
- The **Browser** panel lists a **Databricks** entry.

## Update or remove
- **Update:** Plugins Manager → **Upgradeable**, or install the newer ZIP over the old one.
- **Remove:** Plugins Manager → **Installed** → select the plugin → **Uninstall Plugin**. Saved connections stay in your QGIS settings until you delete them in the plugin.
