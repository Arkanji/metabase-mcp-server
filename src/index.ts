#!/usr/bin/env node

/**
 * Metabase MCP Server
 *
 * Read-only access to any Metabase instance via the Model Context Protocol.
 * 14 tools: 5 Discovery, 5 Querying, 2 Dashboards, 1 Cross-DB, 1 Export.
 *
 * Authentication: METABASE_API_KEY environment variable (x-api-key header)
 * Instance URL: METABASE_URL environment variable
 * Transport: stdio (standard MCP protocol)
 *
 * All queries hit live databases. Tool descriptions guide agents toward
 * efficient patterns (aggregations, breakouts) over row-level scans.
 *
 * @module index
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { MetabaseClient } from './metabase-client.js';
import { crossDbOverlap } from './cross-db.js';
import { exportDataset } from './export.js';

// ─── Configuration ──────────────────────────────────────────────────────────

const metabaseUrl = process.env.METABASE_URL;
if (!metabaseUrl) {
  console.error('Error: METABASE_URL environment variable is required.');
  console.error('Set it to your Metabase instance URL (e.g. https://metabase.example.com).');
  process.exit(1);
}

const apiKey = process.env.METABASE_API_KEY;
if (!apiKey) {
  console.error('Error: METABASE_API_KEY environment variable is required.');
  console.error('Each user must set their own API key. Do not hardcode or commit keys.');
  process.exit(1);
}

const baseUrl = metabaseUrl.replace(/\/+$/, '') + '/api';
const client = new MetabaseClient(baseUrl, apiKey);

// ─── Response Helpers ───────────────────────────────────────────────────────

/** Safely extract an error message from an unknown thrown value */
function extractErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === 'string') return err;
  return String(err);
}

function textResult(data: unknown) {
  return {
    content: [{
      type: 'text' as const,
      text: typeof data === 'string' ? data : JSON.stringify(data, null, 2),
    }],
  };
}

function errorResult(message: string) {
  return {
    content: [{ type: 'text' as const, text: `Error: ${message}` }],
    isError: true as const,
  };
}

// ─── Server Setup ───────────────────────────────────────────────────────────

const server = new McpServer({
  name: 'metabase',
  version: '1.0.0',
});

// ═══════════════════════════════════════════════════════════════════════════
// DISCOVERY TOOLS (1-5)
// ═══════════════════════════════════════════════════════════════════════════

server.tool(
  'list_databases',
  'List all databases connected to Metabase with their ID, name, and engine type.',
  {},
  async (_args, extra) => {
    try {
      const dbs = await client.listDatabases(extra?.signal);
      return textResult(dbs);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'list_tables',
  'List all tables in a database. Returns table ID, name, and schema.',
  {
    database_id: z.number().describe('The database ID (use list_databases to find it)'),
  },
  async (args, extra) => {
    try {
      const tables = await client.listTables(args.database_id, extra?.signal);
      return textResult(tables);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'get_table_fields',
  'Get field IDs, names, and types for a table. You MUST call this before building query_dataset calls — field IDs are required for filters and aggregations.',
  {
    table_id: z.number().describe('The table ID (use list_tables to find it)'),
  },
  async (args, extra) => {
    try {
      const fields = await client.getTableFields(args.table_id, extra?.signal);
      return textResult(fields);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'preview_table',
  'Return a small sample of rows from a table to understand its data shape. Useful for inspecting JSON column structures or understanding field formats before writing queries.',
  {
    database_id: z.number().describe('The database ID'),
    table_id: z.number().describe('The table ID'),
    limit: z.number().min(1).max(50).default(10).describe('Number of rows to return (default 10, max 50)'),
  },
  async (args, extra) => {
    try {
      const query = { 'source-table': args.table_id, limit: args.limit };
      const result = await client.queryDataset(args.database_id, query, extra?.signal);
      return textResult(result);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'search',
  'Search across questions, dashboards, and tables by keyword.',
  {
    query: z.string().max(1000).describe('Search keyword'),
    type: z.enum(['card', 'dashboard', 'table']).optional().describe('Filter by type: card = saved question, dashboard, table'),
  },
  async (args, extra) => {
    try {
      const results = await client.search(args.query, args.type, extra?.signal);
      return textResult(results);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

// ═══════════════════════════════════════════════════════════════════════════
// QUERYING TOOLS (6-10)
// ═══════════════════════════════════════════════════════════════════════════

server.tool(
  'query_dataset',
  'Run an MBQL query against a Metabase database. Metabase enforces a hard server-side cap of 2000 rows — use aggregations to work around this. If is_truncated is true and you need more rows, call again with offset: 2000, then 4000, etc. Only do this for small result sets (<10K rows). For larger exports, use export_dataset instead. For time comparisons (WoW, MoM, trends), use a single query with a date breakout instead of multiple queries. MBQL examples: aggregation: [["count"]] or [["sum", ["field", 87, null]]]. filter: ["=", ["field", 10, null], "active"]. breakout: [["field", 42, {"temporal-unit": "month"}]].',
  {
    database_id: z.number().describe('The database ID'),
    table_id: z.number().describe('The table ID (maps to source-table in MBQL)'),
    aggregation: z.array(z.any()).optional().describe('MBQL aggregation clauses, e.g. [["count"], ["sum", ["field", 87, null]]]'),
    filter: z.array(z.any()).optional().describe('MBQL filter clause, e.g. ["=", ["field", 135, null], 4]'),
    breakout: z.array(z.any()).optional().describe('MBQL breakout fields for grouping, e.g. [["field", 91, null]]'),
    limit: z.number().max(2000).default(2000).describe('Row limit (max/default 2000 — Metabase hard cap)'),
    offset: z.number().default(0).describe('Offset for pagination (use with limit for small result sets <10K rows)'),
  },
  async (args, extra) => {
    try {
      const query: Record<string, unknown> = {
        'source-table': args.table_id,
      };
      if (args.aggregation) query.aggregation = args.aggregation;
      if (args.filter) query.filter = args.filter;
      if (args.breakout) query.breakout = args.breakout;
      if (args.limit !== undefined) query.limit = args.limit;
      if (args.offset !== undefined && args.offset > 0) query.offset = args.offset;

      const result = await client.queryDataset(args.database_id, query, extra?.signal);
      return textResult({
        ...result,
        is_truncated: result.row_count >= (args.limit || 2000),
      });
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'run_native_query',
  'Run a native SQL query against a Metabase database. Requires that your API key has native query (SQL) permission in Metabase. IMPORTANT: You must include a LIMIT clause in your SQL to avoid large result sets. Write operations (INSERT, UPDATE, DELETE, DROP, etc.) and multi-statement queries are rejected. The server strips SQL comments before validation.',
  {
    database_id: z.number().describe('The database ID'),
    query: z.string().max(100_000).describe('The SQL query to execute. Include a LIMIT clause to control result size.'),
  },
  async (args, extra) => {
    try {
      const result = await client.runNativeQuery(args.database_id, args.query, extra?.signal);
      return textResult(result);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'run_saved_question',
  'Execute a saved question (card) by ID. Works for both MBQL and native SQL cards.',
  {
    card_id: z.number().describe('The saved question/card ID'),
  },
  async (args, extra) => {
    try {
      const result = await client.runSavedQuestion(args.card_id, extra?.signal);
      return textResult({
        ...result,
        is_truncated: result.row_count >= 2000,
      });
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'get_card_metadata',
  'Get a saved question\'s definition (query type, database, SQL/MBQL) without executing it.',
  {
    card_id: z.number().describe('The saved question/card ID'),
  },
  async (args, extra) => {
    try {
      const metadata = await client.getCardMetadata(args.card_id, extra?.signal);
      return textResult(metadata);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'list_saved_questions',
  'List all saved questions/cards, optionally filtered by database.',
  {
    database_id: z.number().optional().describe('Filter to cards for a specific database ID'),
  },
  async (args, extra) => {
    try {
      let cards = await client.listCards(extra?.signal);
      if (args.database_id) {
        cards = cards.filter((c) => c.database_id === args.database_id);
      }
      return textResult(cards);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

// ═══════════════════════════════════════════════════════════════════════════
// DASHBOARD TOOLS (11-12)
// ═══════════════════════════════════════════════════════════════════════════

server.tool(
  'list_dashboards',
  'List all dashboards.',
  {},
  async (_args, extra) => {
    try {
      const dashboards = await client.listDashboards(extra?.signal);
      return textResult(dashboards);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

server.tool(
  'get_dashboard',
  'Get a dashboard\'s cards and layout.',
  {
    dashboard_id: z.number().describe('The dashboard ID'),
  },
  async (args, extra) => {
    try {
      const dashboard = await client.getDashboard(args.dashboard_id, extra?.signal);
      return textResult(dashboard);
    } catch (err: unknown) {
      return errorResult(extractErrorMessage(err));
    }
  }
);

// ═══════════════════════════════════════════════════════════════════════════
// CROSS-DB ANALYSIS TOOL (13)
// ═══════════════════════════════════════════════════════════════════════════

const sourceSchema = z.object({
  database_id: z.number(),
  table_id: z.number(),
  field_id: z.number(),
});

server.tool(
  'cross_db_overlap',
  'Compare a field across two databases to find overlapping values (e.g. find which email addresses exist in both your CRM and marketing databases). Handles cross-DB join limitations by fetching both sets and computing intersection in memory. Caps at 500K rows per side — add filters for larger tables. Supports cancellation.',
  {
    source_a: sourceSchema.describe('First source: { database_id, table_id, field_id }'),
    source_b: sourceSchema.describe('Second source: { database_id, table_id, field_id }'),
    filter_a: z.array(z.any()).optional().describe('MBQL filter for source A'),
    filter_b: z.array(z.any()).optional().describe('MBQL filter for source B'),
    sample_size: z.number().min(0).max(50).default(0).describe('Return this many sample overlapping values (default 0, max 50)'),
  },
  async (args, extra) => {
    const signal = extra?.signal;

    try {
      const result = await crossDbOverlap(client, {
        source_a: args.source_a,
        source_b: args.source_b,
        filter_a: args.filter_a,
        filter_b: args.filter_b,
        sample_size: args.sample_size,
      }, signal);

      return textResult(result);
    } catch (err: unknown) {
      if ((err as Error).name === 'AbortError') {
        return errorResult('Operation cancelled. All in-flight Metabase requests have been aborted.');
      }
      return errorResult(extractErrorMessage(err));
    }
  }
);

// ═══════════════════════════════════════════════════════════════════════════
// DATA EXPORT TOOL (14)
// ═══════════════════════════════════════════════════════════════════════════

server.tool(
  'export_dataset',
  'Export full row-level data to a local CSV or JSON file. Auto-paginates through the 2000-row Metabase cap internally. The file path is returned in the response. Use this when you need complete data for external analysis (Excel, Google Sheets). For analytical queries, use query_dataset with aggregations instead. Caps at 500K rows — add filters for larger tables. Output directory is configurable via METABASE_EXPORT_DIR env var (defaults to ~/Downloads/).',
  {
    database_id: z.number().describe('The database ID'),
    table_id: z.number().describe('The table ID'),
    fields: z.array(z.number()).optional().describe('Field IDs to include (omit for all fields)'),
    filter: z.array(z.any()).optional().describe('MBQL filter clause'),
    order_by: z.array(z.any()).optional().describe('MBQL order-by clause'),
    format: z.enum(['csv', 'json']).default('csv').describe('Output format: csv (default) or json'),
  },
  async (args, extra) => {
    const signal = extra?.signal;

    try {
      const result = await exportDataset(client, {
        database_id: args.database_id,
        table_id: args.table_id,
        fields: args.fields,
        filter: args.filter,
        order_by: args.order_by,
        format: args.format,
      }, signal);

      return textResult(result);
    } catch (err: unknown) {
      if ((err as Error).name === 'AbortError') {
        return errorResult('Export cancelled. Partial file may remain on disk.');
      }
      return errorResult(extractErrorMessage(err));
    }
  }
);

// ─── Server Start ───────────────────────────────────────────────────────────

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('Metabase MCP server running on stdio');
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
