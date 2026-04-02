/**
 * Dataset Export Module
 *
 * Exports full row-level data from Metabase to local CSV or JSON files.
 * Handles Metabase's 2000-row hard cap by auto-paginating internally
 * and appending each batch directly to disk (batched synchronous writes).
 *
 * Output directory: Configurable via METABASE_EXPORT_DIR env var,
 * defaults to ~/Downloads/.
 *
 * @module export
 */

import { writeFileSync, appendFileSync, statSync, existsSync, mkdirSync, unlinkSync } from 'fs';
import { homedir } from 'os';
import { join, resolve } from 'path';
import { MetabaseClient } from './metabase-client.js';

const MAX_ROWS = 500_000;
/** Rows above this count trigger a warning in the response (not a hard limit) */
const WARN_THRESHOLD = 100_000;
const BATCH_SIZE = 2000;

export interface ExportParams {
  database_id: number;
  table_id: number;
  fields?: number[];
  filter?: unknown;
  order_by?: unknown;
  format?: 'csv' | 'json';
}

export interface ExportResult {
  file_path: string;
  row_count: number;
  columns: string[];
  file_size_mb: number;
  elapsed_seconds: number;
  warning?: string;
}

interface ExportError {
  error: string;
  row_count: number;
  suggestion: string;
}

function getExportPath(format: string): string {
  const rawDir = process.env.METABASE_EXPORT_DIR || join(homedir(), 'Downloads');
  const dir = resolve(rawDir);
  if (dir.includes('..')) {
    throw new Error(`Export directory must not contain '..': ${rawDir}`);
  }
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true, mode: 0o750 });
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  return join(dir, `metabase-export-${timestamp}.${format}`);
}

function escapeCSV(value: unknown): string {
  if (value == null) return '';
  const str = String(value);
  if (str.includes(',') || str.includes('"') || str.includes('\n') || str.includes('\r')) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

export async function exportDataset(
  client: MetabaseClient,
  params: ExportParams,
  signal?: AbortSignal
): Promise<ExportResult | ExportError> {
  const startTime = Date.now();
  const format = params.format || 'csv';

  signal?.throwIfAborted();

  console.error('[export_dataset] Counting rows...');
  const countQuery: Record<string, unknown> = {
    'source-table': params.table_id,
    aggregation: [['count']],
  };
  if (params.filter) countQuery.filter = params.filter;

  const countResult = await client.queryDataset(params.database_id, countQuery, signal);
  const totalRows = (countResult.rows[0]?.[0] as number) || 0;

  if (totalRows > MAX_ROWS) {
    return {
      error: `Query would return ${totalRows.toLocaleString()} rows, exceeding the ${MAX_ROWS.toLocaleString()} export limit. Add filters to narrow the dataset.`,
      row_count: totalRows,
      suggestion: 'Try filtering by date range, status, or other criteria to reduce the result set.',
    };
  }

  if (totalRows === 0) {
    return {
      error: 'Query returned 0 rows. Check your filters.',
      row_count: 0,
      suggestion: 'Verify the table_id and filter are correct using preview_table first.',
    };
  }

  console.error(`[export_dataset] ${totalRows.toLocaleString()} rows to export. Starting...`);

  const filePath = getExportPath(format);
  let columns: string[] = [];
  let writtenRows = 0;
  let offset = 0;

  try {
    if (format === 'json') {
      writeFileSync(filePath, '[\n', 'utf-8');
    }

    while (offset < totalRows) {
      signal?.throwIfAborted();

      const query: Record<string, unknown> = {
        'source-table': params.table_id,
        limit: BATCH_SIZE,
        offset,
      };
      if (params.fields) query.fields = params.fields.map((f) => ['field', f, null]);
      if (params.filter) query.filter = params.filter;
      if (params.order_by) query['order-by'] = params.order_by;

      const result = await client.queryDataset(params.database_id, query, signal);

      if (offset === 0) {
        columns = result.columns;
        if (format === 'csv') {
          writeFileSync(filePath, columns.map(escapeCSV).join(',') + '\n', 'utf-8');
        }
      }

      if (format === 'csv') {
        const csvLines = result.rows.map((row) =>
          (row as unknown[]).map(escapeCSV).join(',')
        ).join('\n');
        appendFileSync(filePath, csvLines + '\n', 'utf-8');
      } else {
        const jsonLines = result.rows.map((row, i) => {
          const obj: Record<string, unknown> = {};
          columns.forEach((col, ci) => { obj[col] = (row as unknown[])[ci]; });
          const prefix = (writtenRows + i > 0) ? ',' : '';
          return prefix + JSON.stringify(obj);
        }).join('\n');
        appendFileSync(filePath, jsonLines + '\n', 'utf-8');
      }

      writtenRows += result.row_count;
      offset += BATCH_SIZE;

      if (writtenRows % 10000 < BATCH_SIZE || writtenRows >= totalRows) {
        console.error(`[export_dataset] Exported ${writtenRows.toLocaleString()}/${totalRows.toLocaleString()} rows`);
      }

      if (result.row_count < BATCH_SIZE) break;
    }

    if (format === 'json') {
      appendFileSync(filePath, ']\n', 'utf-8');
    }
  } catch (err) {
    // Attempt to close JSON array so partial file is at least parseable
    if (format === 'json' && existsSync(filePath)) {
      try { appendFileSync(filePath, ']\n', 'utf-8'); } catch { /* best-effort */ }
    }
    throw err;
  }

  let fileSizeMb = 0;
  try {
    fileSizeMb = Math.round((statSync(filePath).size / 1024 / 1024) * 100) / 100;
  } catch { /* non-critical */ }
  const elapsedSeconds = Math.round((Date.now() - startTime) / 1000);

  const exportResult: ExportResult = {
    file_path: filePath,
    row_count: writtenRows,
    columns,
    file_size_mb: fileSizeMb,
    elapsed_seconds: elapsedSeconds,
  };

  if (writtenRows !== totalRows) {
    exportResult.warning = `Expected ${totalRows.toLocaleString()} rows but exported ${writtenRows.toLocaleString()}. Data may have changed during export.`;
  } else if (totalRows > WARN_THRESHOLD) {
    exportResult.warning = `Large export — ${totalRows.toLocaleString()} rows`;
  }

  console.error(`[export_dataset] Done in ${elapsedSeconds}s. File: ${filePath} (${fileSizeMb} MB)`);
  return exportResult;
}
