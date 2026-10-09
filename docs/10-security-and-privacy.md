# 10. Security and privacy

[← Basemaps and styling](09-basemaps-and-styling.md) · [User guide](README.md) · Next: [Troubleshooting →](11-troubleshooting.md)

## Where your credentials are stored

| What | Where | Notes |
|---|---|---|
| Saved connections (host, HTTP path, auth method) | Your QGIS user settings | Per QGIS profile |
| **Personal access token** | Your QGIS user settings, with the connection | Plain text in your settings, readable by anyone who can read your user profile |
| **OAuth sign-in** | `databricks_oauth_tokens.json` in your QGIS profile folder | Readable only by your user account (permissions `0600`). Refreshed automatically |

**Recommendation:** use **OAuth** where you can. You don't keep a long-lived token, sign-ins refresh themselves, and access follows your normal Databricks login (including SSO and MFA).

## What goes into a QGIS project file (`.qgz`)

| Layer type | Stored in the project | Token in the project? |
|---|---|---|
| [Saved in project](06-custom-queries.md#save-a-query-in-your-project) (Custom Query) | Workspace host, HTTP path, the SQL or table, the **connection name**, auth method | **Never** |
| Temporary or live layers added from the main dialog or Browser | Connection details in the layer's properties | **Personal-access-token connections: yes**. OAuth: no |

> **Before sharing a project that contains temporary layers loaded with a personal access token,** remove those layers or re-create them with **Save in project** or OAuth. Otherwise the token travels with the `.qgz`.

## Permissions
- Everything you load, query or ask Genie about runs **as you**, with your Unity Catalog permissions.
- When a colleague opens your project, saved-in-project layers re-query **with their permissions**. They may see fewer rows, or none.
- **Execute Query** in Custom Query runs any statement you type, including ones that change data, with your permissions. *Save in project* only accepts a single `SELECT`.

## Network traffic
The plugin talks only to your Databricks workspace over HTTPS (SQL warehouse, Genie APIs, OAuth sign-in), plus the Python package index when it installs its dependency. Basemaps load from the tile provider you choose.

## Signing out
- **OAuth:** delete `databricks_oauth_tokens.json` from your QGIS profile folder (**Settings → User Profiles → Open Active Profile Folder**). The next connection asks you to sign in again.
- **Personal access token:** delete the connection (**Delete Connection**) and revoke the token in Databricks (**Settings → Developer → Access tokens**).
