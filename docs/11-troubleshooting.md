# 11. Troubleshooting

[← Security and privacy](10-security-and-privacy.md) · [User guide](README.md)

## Find the logs first
**View → Panels → Log Messages**, then the **Databricks Connector**, **Databricks Provider** and **Query Dialog** tabs. Include this text when you [report an issue](https://github.com/danny-db/qgis-databricks-connector/issues).

## Install

| Symptom | Fix |
|---|---|
| *"Installation failed … Connection refused"* to `pypi.org` | Your network blocks the public package index. Configure your organisation's PyPI mirror: see [Install](01-install.md#install-the-python-dependency-first-run-only) |
| Plugin loaded but nothing works after installing the dependency | Restart QGIS; the package is only picked up after a restart |

## Can't connect

| Symptom | Fix |
|---|---|
| *"Connection failed"* | Check **Server Hostname** (no `https://`) and **HTTP Path**; make sure the SQL warehouse exists and you can use it. A stopped serverless warehouse starts automatically, so wait a few seconds and try again |
| Authentication error with a token | The token may be expired or revoked. Generate a new one and save the connection again |
| **Sign in** doesn't open a browser | QGIS needs a desktop session (it won't work over a headless remote session). Check nothing else is using local port `8020`, which receives the sign-in |
| Signed in but later connections fail | Delete `databricks_oauth_tokens.json` from your profile folder (**Settings → User Profiles → Open Active Profile Folder**) and click **Sign in** again |

## Layers

| Symptom | Fix |
|---|---|
| **No spatial tables found** | You need `SELECT` permission and a `GEOMETRY`/`GEOGRAPHY` column. For lat/long tables, use a [custom query](06-custom-queries.md) with `ST_POINT` |
| A saved-in-project layer shows in **Handle Unavailable Layers** when the project opens | Its saved connection isn't on this computer. Create a connection **with the same name** (see [Share a project](06-custom-queries.md#share-a-project-with-saved-queries)), then reopen the project |
| *"Save this connection first…"* | Personal-access-token connections must be saved before **Save in project**. Click **Save Connection** in the main dialog |
| *"Only a single SELECT … can be saved as a layer"* | Save in project accepts one `SELECT` or `WITH … SELECT` statement |
| Temporary **Add as Layer** fails on a query without geometry | Use **Save in project** for summary tables |
| Saved layer draws nothing | Zoom to it (right-click → **Zoom to Layer(s)**). Check the query returns a geometry column, and that the coordinates match the coordinate system (e.g. `ST_POINT(longitude, latitude, 4326)`; longitude first) |
| Layer is slow when zoomed right out | It's fetching every feature in view. Zoom in, filter in your SQL, or aggregate (e.g. H3 cells) |

## Genie

| Symptom | Fix |
|---|---|
| **Genie Agent** list is empty | You don't have access to any Genie Agents in this workspace; ask your data team |
| *"Genie One is not available on this workspace"* | Genie One isn't enabled for this workspace |
| *"authentication failed (401/403)"* | Sign in again (OAuth) or check your token; your account also needs access to Genie |
| *"Rate limited (429)"* or a temporary request limit | The workspace is busy. Genie One retries automatically; for Genie Agent, wait a moment and ask again |
| No chart shown | Genie decided a chart doesn't help; ask explicitly, e.g. *"…as a bar chart"*. Charts also need matplotlib, which is bundled with most QGIS installs |

## Basemaps

| Symptom | Fix |
|---|---|
| *"API KEY REQUIRED"* tiles | CARTO now needs a key; use OpenStreetMap or Esri instead (see [Basemaps](09-basemaps-and-styling.md)) |
| Blank basemap | Your network may block the tile server; try another provider |
