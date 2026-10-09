# Changelog

What changed in each release of the QGIS Databricks DBSQL Connector, newest first. For how to use each feature, see the [user guide](docs/README.md).

## v1.7.1

- **Installer**: installs the standard `databricks-sql-connector` package
- **Lakehouse Real-Time warehouses**: support is planned for a later release
- **Docs**: README rewritten around outcomes and use cases, and this changelog

## v1.7.0

### Explain this Map (frontier models on Databricks)
- **One click**: the **Explain this Map** toolbar button sends the current map to a frontier model on your Databricks workspace and streams back an explanation
- **No API key, no extra subscription**: it uses your existing Databricks sign-in and Foundation Model APIs
- **Your choice of model**: pick any image-capable model on your workspace (Claude, GPT, Gemini, Llama and more)
- **Prompt presets and custom prompts**: explain, report summary, patterns and outliers, title/caption/alt text, next analysis steps
- **Grounded in your map**: area, scale, layers and what each style encodes are sent with the image, so answers use your real layer and field names
- **Follow-up questions**, copy, and save as Markdown with the map image
- **Smoother sign-in**: OAuth sessions renew well ahead of expiry, so long Genie One and Explain this Map sessions run without interruption

### Official Genie icons
- The **Genie Agent** and **Genie One** toolbar buttons now use the official Genie icons

## v1.6.1

- **Easier, more secure project sharing**: every Databricks layer now links to your saved connection by name, so a project carries only the connection details. Projects from earlier versions are upgraded automatically when you open them
- **Summary tables as layers**: Custom Query **Add as Layer** now also works for queries without a geometry column

## v1.6.0

### Save Custom SQL Queries in the Project (issue #3)
- **Save in project**: In Custom Query, tick **Save in project** before **Add as Layer**. The query is stored in the `.qgz` and re-runs against Databricks every time the project is opened, like a PostGIS SQL layer
- **Only fetches what you see**: The layer queries Databricks for the visible map area, so large results stay responsive
- **Easy to share**: the project stores the SQL and the **saved connection's name**, so colleagues open it with their own sign-in. OAuth layers also open for colleagues without the same saved connection
- **Select, identify and refresh work**: Stable feature ids; *Refresh* re-reads the query
- **Fixed**: the plugin's Databricks data provider now recognises `GEOMETRY(n)` / `GEOGRAPHY(n)` columns, reads the extent, and handles Multi* geometry types correctly

## v1.5.1

- **Security hardening**: stronger SQL handling (names of any kind quoted safely, map extents sent as query parameters), HTTPS for every Databricks API call, and clearer diagnostics in the QGIS log. Passes the QGIS plugin site security checks
- **Qt6 ready**: modern, fully scoped Qt/QGIS code throughout, so the same plugin runs natively on QGIS 3 (Qt5) and QGIS 4 (Qt6)

## v1.5.0

### Genie One (Ask Across Your Whole Workspace)
- **New Genie One dialog**: `Plugins → Databricks DBSQL Connector → Databricks Genie One` (or the chat-bubble toolbar icon). Ask in plain English and Genie One finds the right data across the workspace, with no Genie Agent to pick
- **Live progress**: Genie One's steps (searching tables, running SQL) show while it works; **Cancel** stops the request
- **Every query, on the map**: Genie One may run several queries per answer. Pick one from the **Result** dropdown, then **Add as Layer** for spatial results
- **Open in Databricks**: Jump to the same conversation in Genie One in your browser
- **Works with both auth methods**: Personal Access Token or OAuth sign-in; uses the Genie One MCP server on Unity Gateway (`/ai-gateway/mcp-services/system.ai.genie_one_mcp`)

### Charts in the Chat (Genie Agent + Genie One)
- **Genie's own visualisations, inline**: When Genie draws a chart, it now appears in the chat just like the Databricks UI
- **Genie Agent**: shows the exact chart image Genie renders
- **Genie One**: draws Genie One's chart definition (bar, line, area, scatter, pie; dual axes, stacking, number formats) right where the answer places it
- **Auto chart**: If Genie returns chart-shaped data without a chart, the plugin draws a simple one (labelled as such)
- **Save Chart...**: Save the latest chart as a PNG

### Genie Chat is now Genie Agent
- The Genie Chat dialog is renamed **Databricks Genie Agent** to match Databricks naming (Genie Spaces are now Genie Agents). It works exactly as before

## v1.4.0

### OAuth Authentication (Browser Login / SSO)
- **Auth Method selector**: Choose **Personal Access Token** or **OAuth (browser login / SSO)** per connection
- **No token required for OAuth**: Sign in through your browser (including SSO/identity provider) — the plugin caches and refreshes the session automatically, so the browser opens only once
- **Sign in button**: Prime the OAuth flow up front before discovering tables or loading layers
- **Reused everywhere**: The cached OAuth session powers Test Connection, table discovery, layer loads, live-layer refreshes, the Browser panel, and Genie
- **Secure-by-default storage**: Tokens are written to a permission-restricted file under your QGIS profile directory
- **Fully backwards compatible**: Existing Personal Access Token connections and saved layers keep working unchanged

## v1.3.1

- **Fixed**: dependency installation on QGIS 4 macOS (in-process pip with `--no-build-isolation`, so builds no longer spawn a broken subprocess or a second QGIS)
- **Fixed**: "Couldn't load SIP module" during dependency installation (QGIS paths are kept out of build subprocesses)

## v1.3.0

### Genie Chat (Natural Language Data Queries)
- **Databricks Genie Chat**: Ask questions about your data in plain English — Genie translates them to SQL and returns results
- **Genie Space browser**: Select from available Genie Spaces (auto-populated from your workspace)
- **Conversation history**: Follow-up questions are sent within the same conversation for context-aware answers
- **Thinking indicator**: Animated status with elapsed time while Genie processes your question; Cancel button to abort
- **Smart geometry hints**: Automatically instructs Genie to return geometry as a typed column, improving layer creation success
- **Results preview**: View query results in a table (up to 100 rows shown, full dataset held in memory)
- **Add as Layer**: Auto-detect geometry columns and create QGIS memory layers from Genie results
- **Mixed geometry support**: Creates separate layers per geometry type (Point, LineString, Polygon)
- **Non-WKT geometry handling**: Automatically re-queries with `ST_ASWKT()` wrapping when needed
- **Collapsible SQL panel**: Generated SQL hidden by default — click "Show SQL" to reveal, with "Copy SQL" alongside
- **Markdown rendering**: Genie responses with bold, italic, bullet lists, and code render correctly in the chat
- **Dark mode compatible**: Explicit text colours ensure readability in both light and dark QGIS themes
- **Toolbar + menu access**: Click the Databricks icon or use `Plugins → Databricks → Databricks Genie Chat`

## v1.2.0

### Live Layers
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

## v1.1.1

- **Safer catalog lookups**: `information_schema` queries use parameters, and escaped SQL identifiers are documented

## v1.1.0

- **Update Layer Data from Databricks**: refresh one or several selected layers with fresh data
- **Fixed**: column names with spaces, Polygon/MultiPolygon layers from the Browser panel, and DateTime fields
- **Improved**: the dialog and the Browser panel behave the same way

## v1.0.0

- Direct connection to Databricks SQL warehouses
- Browser panel integration for browsing Unity Catalog
- Custom SQL query dialog with a database structure browser
- GEOGRAPHY and GEOMETRY data types, with mixed geometry tables split into separate layers
- Configurable layer prefix and feature limits; saved connections
