# Manual test plan: Save custom SQL queries in the project (issue #3, v1.6.0)

Branch `feature/issue-3-sql-project-layers`. Install the zip built from this branch: `databricks_dbsql_connector.zip` (v1.6.0).

## Before you start
- **QGIS 3.x and/or 4.x**, with the plugin installed from the v1.6.0 zip (Plugins → Manage and Install Plugins → Install from ZIP; restart QGIS).
- **Two saved connections** in the Databricks dialog:
  - `my-oauth`: Auth Method **OAuth**; click **Sign in** once.
  - `my-pat`: Auth Method **Personal Access Token**, with a valid `dapi…` token.
- A table with a geometry column. On fe-vm-vdm-serverless-dw-demo, any table from **Discover Tables** works. Example query:
  ```sql
  SELECT parcel_id, tier, score, geom
  FROM `catalog`.`schema`.`table`
  WHERE score > 50
  ```
- Add a basemap (OSM or Esri Light Gray) so you can see where features draw.

## Test cases

| # | Steps | Expected |
|---|---|---|
| 1 | Open **Custom Query** from the dialog with `my-oauth` loaded. Run the query. Leave **Save in project** unticked → **Add as Layer** | A temporary layer, as before (no change in behaviour) |
| 2 | Same query, tick **Save in project** → **Add as Layer** | Message: layer added, "saved with the project". The layer draws. Layer Properties → Information shows the provider as **databricks** and a source containing `sql=` and `conn=my-oauth` |
| 3 | Zoom in and out, and pan around | Features redraw for each view. Zoomed in, only nearby features load (fast) |
| 4 | Use **Identify** on a feature; select a few features; open the attribute table | Identify shows the right attributes; the selection highlights the same features; the attribute table lists rows |
| 5 | **Save the project** (`.qgz`), close QGIS, reopen QGIS and the project | The layer comes back automatically and draws, with no "unavailable layer" prompt |
| 6 | Unzip the `.qgz` and search the `.qgs` for `dapi` | **No match.** You'll see the SQL and `conn=…` but no token |
| 7 | Load `my-pat`, run the query, tick **Save in project** → **Add as Layer**; save, close and reopen the project | Works like case 5. The `.qgs` has `conn=my-pat` and **no token** |
| 8 | Type a new PAT connection **without saving it**. Custom Query → **Save in project** → **Add as Layer** | Warning "Save this connection first…"; no layer is added |
| 9 | From the **Browser panel**: right-click a connection → Custom Query → **Save in project** | Same as case 2 (the connection name comes from the Browser) |
| 10 | Edit the SQL to `DROP TABLE x` (or two statements separated by `;`) → **Save in project** | Refused: "Only a single SELECT…" |
| 11 | Query **without a geometry column** (e.g. `SELECT tier, COUNT(*) n FROM … GROUP BY tier`) → **Save in project** | An attribute-only layer (table icon); the attribute table shows the rows |
| 12 | Right-click the layer → **Refresh**, after changing data in Databricks if you can | New or changed rows appear |
| 13 | Rename or delete the saved connection `my-oauth`, then reopen the project from case 5 | The OAuth layer still opens: it falls back to the workspace host and path with your own sign-in |
| 14 | Delete `my-pat`, then reopen the project from case 7 | The PAT layer shows as **unavailable** (QGIS's Handle Unavailable Layers dialog), with no crash. Re-create `my-pat` with the same name and reopen: the layer works |
| 15 | Send the `.qgz` from case 5 to a colleague who has the plugin and OAuth access | They open it, sign in, and see the layer with their own permissions |
| 16 | Regression: Discover Tables → load a table; Live Layer; Genie Agent; Genie One | Everything behaves as in v1.5.1 |

Run cases 2, 5, 6 and 7 on **both QGIS 3 and QGIS 4** if you can.

## Known limitations (by design)
- The plugin must be installed for a saved project layer to open; without it, QGIS reports the layer as unavailable.
- Project layers are read-only; editing goes through Databricks.
- One geometry column per layer: the first one, or the one typed in **Geometry column**.
- Very large results: zoomed out to the full extent, a query can return every feature. Zoom in for big tables.

## Report back
For any failure, send the case number, QGIS version, auth method, and the **Log Messages** panel → **Databricks Provider** tab text.
