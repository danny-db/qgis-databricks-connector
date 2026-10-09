# 4. Browse catalogs in the Browser panel

[← Load tables](03-load-tables.md) · [User guide](README.md) · Next: [Live layers →](05-live-layers.md)

Every saved connection appears in the QGIS **Browser** panel, so you can explore Unity Catalog without opening the dialog.

## Navigate
1. Show the panel if it's hidden: **View → Panels → Browser**.
2. Expand **Databricks** → your connection → a catalog → a schema → a table.
3. Expanding a table lists its columns.
4. Each connection also has a **⚡ Custom Query** item for writing your own SQL.

Connections appear here after you **Save** them in the main dialog ([Connect](02-connect.md)). If a new connection doesn't show, right-click **Databricks** → **Refresh**.

## Right-click a table

| Menu item | What it does |
|---|---|
| **Add All Features** | Adds the whole table as a layer |
| **Add First 1000 Features** | Adds a quick sample, ideal for big tables |
| **Add as Live Layer (Viewport)** | Adds a [live layer](05-live-layers.md) that loads only what's on screen |
| **View Data...** | Opens [Custom Query](06-custom-queries.md) with `SELECT * FROM <table> LIMIT 100` filled in. Edit and run it, or save it in your project |

## Right-click a connection or ⚡ Custom Query

| Menu item | What it does |
|---|---|
| **Execute Custom Query...** | Opens [Custom Query](06-custom-queries.md) for that connection. **Save in project** works from here too |
| **Execute Query...** (on **⚡ Custom Query**) | Opens [Custom Query](06-custom-queries.md) for that connection |
| **Refresh** | Re-reads catalogs and schemas |
