<h1 align="center">Metabase MCP Server</h1>

<p align="center">
  Connect any AI agent to your Metabase instance.<br/>
  Read-only tools &middot; MBQL + native SQL &middot; cross-DB analysis &middot; data export
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="MIT License"/></a>
  <img src="https://img.shields.io/badge/Node.js-18%2B-green.svg" alt="Node 18+"/>
  <img src="https://img.shields.io/badge/MCP-compatible-purple.svg" alt="MCP Compatible"/>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="#tools">Tools</a> &middot;
  <a href="#configuration">Configuration</a> &middot;
  <a href="#examples">Examples</a> &middot;
  <a href="#troubleshooting">Troubleshooting</a>
</p>

---

An [MCP](https://modelcontextprotocol.io) server that gives AI agents read-only access to any [Metabase](https://www.metabase.com) instance. Explore schemas, query databases, run saved questions, compare data across databases, and export results — all through natural conversation.

Works with Claude Code, Cursor, and any MCP-compatible client.

## Quick Start

```bash
git clone https://github.com/arkanji/metabase-mcp-server.git
cd metabase-mcp-server
npm install && npm run build
```

Then add it to your AI client:

<details>
<summary><strong>Claude Code</strong> — <code>.mcp.json</code></summary>

```json
{
  "mcpServers": {
    "metabase": {
      "command": "node",
      "args": ["/path/to/metabase-mcp-server/dist/index.js"],
      "env": {
        "METABASE_URL": "https://metabase.example.com",
        "METABASE_API_KEY": "mb_xxxxxxxxxxxxx"
      }
    }
  }
}
```

</details>

<details>
<summary><strong>Cursor</strong> — <code>.cursor/mcp.json</code></summary>

```json
{
  "mcpServers": {
    "metabase": {
      "command": "node",
      "args": ["/path/to/metabase-mcp-server/dist/index.js"],
      "env": {
        "METABASE_URL": "https://metabase.example.com",
        "METABASE_API_KEY": "mb_xxxxxxxxxxxxx"
      }
    }
  }
}
```

</details>

## Configuration

| Variable | Required | Default | Description |
|:---------|:--------:|:-------:|:------------|
| `METABASE_URL` | Yes | — | Your Metabase instance URL. Do **not** include `/api`. |
| `METABASE_API_KEY` | Yes | — | [How to generate one](https://www.metabase.com/docs/latest/people-and-groups/api-keys) — go to Admin > Settings > API Keys. |
| `METABASE_EXPORT_DIR` | No | `~/Downloads/` | Where `export_dataset` writes CSV/JSON files. |

## Tools

### Discovery

| Tool | What it does |
|:-----|:-------------|
| `list_databases` | List all connected databases |
| `list_tables` | List tables in a database |
| `get_table_fields` | Get column names, IDs, and types for a table |
| `preview_table` | Sample rows to understand data shape |
| `search` | Find saved questions, dashboards, or tables by keyword |

### Querying

| Tool | What it does |
|:-----|:-------------|
| `query_dataset` | Run structured MBQL queries (aggregations, filters, breakouts) |
| `run_native_query` | Run raw SQL queries (requires SQL permission on API key) |
| `run_saved_question` | Execute an existing saved question by ID |
| `get_card_metadata` | Inspect a saved question's definition without running it |
| `list_saved_questions` | List all saved questions, optionally by database |

### Dashboards

| Tool | What it does |
|:-----|:-------------|
| `list_dashboards` | List all dashboards |
| `get_dashboard` | Get a dashboard's cards and layout details |

### Analysis & Export

| Tool | What it does |
|:-----|:-------------|
| `cross_db_overlap` | Find overlapping values across two databases (e.g. shared emails between CRM and marketing) |
| `export_dataset` | Export up to 500K rows to CSV/JSON with auto-pagination |

## Examples

**"How many orders did we get last month?"**

The agent will:
1. `list_databases` → find your orders database
2. `list_tables` → find the orders table
3. `get_table_fields` → get the date and status field IDs
4. `query_dataset` → run a count aggregation with a date filter

**"Export all active customers to a CSV"**

The agent will:
1. Discover the table and field IDs
2. `export_dataset` → auto-paginates through the 2,000-row Metabase cap, writes to `~/Downloads/`

**"Which customers exist in both our CRM and marketing databases?"**

The agent will:
1. Identify the email field in both databases
2. `cross_db_overlap` → fetches both sides, computes the intersection in memory

## How It Works

```
You ──→ AI Agent ──→ MCP Server ──→ Metabase REST API ──→ Your Databases
         (Claude,      (this       (x-api-key auth,
          Cursor)       project)    read-only)
```

**Key things to know:**

- **Read-only** — no write operations. Native SQL queries are validated to reject `INSERT`, `UPDATE`, `DELETE`, `DROP`, etc.
- **2,000-row cap** — Metabase limits query results to 2,000 rows. Use aggregations for analytics, or `export_dataset` for full data extraction (auto-paginates up to 500K rows).
- **30s timeout** — each API request times out after 30 seconds to prevent hung connections.
- **MBQL** — Metabase's structured query language. Columns are referenced by field ID (not name), so always call `get_table_fields` first to look them up.

## Security

- **API keys** are passed via environment variables and sent as `x-api-key` headers. They are never logged or exposed in tool responses.
- **Write-operation guard** — `run_native_query` rejects queries that start with write keywords (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`, `CREATE`, `GRANT`, `REVOKE`). This is a basic prefix check, not a comprehensive SQL parser — Metabase's own permission system is the primary access control.
- **No secrets in code** — the server reads credentials only from env vars at startup.

## Troubleshooting

| Problem | Cause | Fix |
|:--------|:------|:----|
| `METABASE_URL environment variable is required` | Missing env var | Set `METABASE_URL` in your `.mcp.json` env block or shell |
| `Metabase API error 401` | Invalid or expired API key | Generate a new key in Metabase Admin > Settings > API Keys |
| `Metabase API error 403` | Key lacks permission for this action | Check your key's permission group in Metabase. Native SQL requires explicit "native query" permission. |
| `Metabase API returned empty response` | URL is wrong or Metabase is down | Verify `METABASE_URL` points to your instance (no trailing `/api`) |
| `Write operations are not allowed` | SQL query starts with INSERT/UPDATE/etc. | This server is read-only. Use SELECT queries only. |
| Queries return empty results | Wrong field IDs or filters | Call `get_table_fields` to verify field IDs, then `preview_table` to check data shape |
| Export hangs | Very large dataset + slow Metabase | Add filters to reduce row count. Exports are capped at 500K rows. |

## Development

```bash
npm run dev      # Watch mode — recompiles on save
npm run build    # One-time build
npm start        # Run the server
```

Should work with any Metabase version that supports the `/api/dataset` endpoint and API key authentication (generally v0.44+).

## Contributing

Issues and PRs are welcome. Please open an [issue](https://github.com/arkanji/metabase-mcp-server/issues) first to discuss significant changes.

## License

[MIT](LICENSE)

---

<p align="center">
  Built by <a href="https://github.com/arkanji">Arkanji</a>
</p>
