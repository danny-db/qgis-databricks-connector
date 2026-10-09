# 5. Live layers

[← Browser panel](04-browser-panel.md) · [User guide](README.md) · Next: [Custom SQL queries →](06-custom-queries.md)

A live layer loads only the features in the current map view, and refreshes automatically as you pan and zoom. Use it for tables too large to load in one go.

## Add a live layer
- **From the main dialog:** tick **Live Mode (auto-refresh on viewport change)** under **Layer Options**, then **Add Selected Layers**.
- **From the Browser panel:** right-click a table → **Add as Live Layer (Viewport)**.

When live mode is ticked, two extra options appear:

| Option | Meaning |
|---|---|
| **Extent Buffer (%)** | Loads a margin around the visible area, so small pans don't trigger a reload |
| **Refresh Delay (ms)** | Waits this long after you stop panning or zooming before querying, to avoid a query per scroll |

**Max Features** still applies per refresh: a cap on how many features one view can load.

## Turn live mode on or off
Select the layer, then **Plugins → Databricks DBSQL Connector → Toggle Live Mode for Layer**.

## Live layers vs. layers saved in a project

| | Live layer | [Saved in project](06-custom-queries.md#save-a-query-in-your-project) |
|---|---|---|
| Loads only what's on screen | Yes | Yes |
| Kept when you reopen the project | No (temporary layer) | **Yes**: re-queries Databricks on open |
| Source | A table | A table or any `SELECT` query |

For layers you want to keep, use **Save in project**.
