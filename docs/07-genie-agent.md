# 7. Genie Agent

[← Custom SQL queries](06-custom-queries.md) · [User guide](README.md) · Next: [Genie One →](08-genie-one.md)

Ask a **Genie Agent** (formerly a *Genie Space*) about your data in plain English. Genie writes the SQL, answers, draws a chart when it helps, and you can put spatial results on the map. A Genie Agent is set up by your data team for a specific topic. To ask across the whole workspace instead, use [Genie One](08-genie-one.md).

## Ask a question
1. Open **Plugins → Databricks DBSQL Connector → Databricks Genie Agent**.
2. **Connection:** pick a saved connection. **Genie Agent:** loads the agents you can use; pick one.
3. Type in **Question**, e.g. *"Show monthly crash counts as a bar chart"*, and click **Ask** (or press Enter).
4. While Genie works you'll see *"Genie is thinking…"* with a timer. Click **Cancel** to stop.
5. The answer appears in the chat:
   - **Text** with key numbers in bold.
   - **Genie's chart** inline, the same image you'd see in Databricks.
   - **The result table** below the chat.

## Use the results
| Button | What it does |
|---|---|
| **Show SQL** / **Copy SQL** | See or copy the SQL Genie ran (reuse it in [Custom Query](06-custom-queries.md), even with **Save in project**) |
| **Geometry col** + **Add as Layer** | Put spatial results on the map. The geometry column is detected automatically |
| **Save Chart...** | Save the latest chart as a PNG |
| **Clear Chat** | Start a new conversation |

## Tips
- **Follow-ups** keep the conversation: *"Now only 2024"*, *"Break that down by LGA"*.
- For **map-ready results**, mention locations: the plugin automatically asks Genie to return geometry as a column when there is any.
- Genie only sees the tables its agent was set up with, and respects your Unity Catalog permissions.
