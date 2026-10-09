# 10. Security and privacy

[← Basemaps and styling](09-basemaps-and-styling.md) · [User guide](README.md) · Next: [Troubleshooting →](11-troubleshooting.md)

The plugin is designed so your credentials stay on your computer, your projects are easy to share, and everything runs with your own Databricks permissions.

## Your credentials stay on your computer

| What | Where it's kept |
|---|---|
| Saved connections (host, HTTP path, auth method) | Your QGIS user profile |
| **OAuth sign-in** | A private file in your QGIS profile folder (`databricks_oauth_tokens.json`), readable only by your user account and refreshed automatically |
| **Personal access token** | Your QGIS user profile, alongside the saved connection |
| A token typed into a connection you haven't saved | Memory only, for the current QGIS session |

**Recommended: OAuth.** You sign in with your normal Databricks login, including SSO and MFA, there's no long-lived token to manage, and the sign-in refreshes itself.

## Projects are easy to share
Every Databricks layer, whether it comes from Discover Tables, the Browser panel, a live layer or a [query saved in the project](06-custom-queries.md#save-a-query-in-your-project), links to your **saved connection by name**. A project file (`.qgz`) carries only the connection details (workspace host, HTTP path, the table or SQL, and the connection name), so you can share it freely.

- **Colleagues using OAuth** open the project and sign in with their own account.
- **Colleagues using a personal access token** create a connection with the **same name** and their own token.
- Projects from earlier versions of the plugin are **upgraded automatically** to this format when you open them, and keep it when you save.

## Everything runs with your permissions
- Loading tables, running queries and asking Genie all use **your** Unity Catalog permissions.
- When a colleague opens your project, layers saved in it re-query Databricks **with their permissions**.
- **Execute Query** in Custom Query runs the statement you type with your permissions, so treat it like any SQL editor. *Save in project* accepts a single `SELECT` statement.

## Secure connections
- All traffic to Databricks (SQL warehouse, Genie, OAuth sign-in) uses **HTTPS**.
- Queries send values such as map extents as **parameters**, and quote table and column names safely.
- The plugin passes the QGIS plugin repository's security checks.
- Apart from Databricks, the plugin only contacts the Python package index (when installing its dependency) and the basemap providers you choose.

## Signing out
- **OAuth:** delete `databricks_oauth_tokens.json` from your QGIS profile folder (**Settings → User Profiles → Open Active Profile Folder**). The next connection asks you to sign in again.
- **Personal access token:** select the connection and click **Delete Connection**. You can also revoke the token in Databricks (**Settings → Developer → Access tokens**).
