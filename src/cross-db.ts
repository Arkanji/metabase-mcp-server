/**
 * Cross-Database Overlap Analysis Module
 *
 * Compares a field across two different Metabase databases to find overlapping values.
 * This solves Metabase's limitation of not supporting cross-DB joins in MBQL.
 *
 * Execution model:
 * 1. Runs count aggregation on both sides (gate: refuses if either side > 500K rows)
 * 2. Paginates through both sets (2000 rows per batch, Metabase's hard cap)
 * 3. Computes set intersection in memory using JavaScript Sets
 * 4. Returns summary stats + optional sample of overlapping values
 *
 * Supports cancellation via AbortController.
 * Memory budget: tens of MBs at 500K rows of short identifiers per side
 * (Set overhead in V8 is significant beyond raw string bytes).
 *
 * @module cross-db
 */

import { MetabaseClient } from './metabase-client.js';

const MAX_ROWS_PER_SIDE = 500_000;
/** Rows above this count trigger a warning in the response (not a hard limit) */
const WARN_THRESHOLD = 100_000;
const BATCH_SIZE = 2000;

export interface OverlapSource {
  database_id: number;
  table_id: number;
  field_id: number;
}

export interface OverlapParams {
  source_a: OverlapSource;
  source_b: OverlapSource;
  filter_a?: unknown;
  filter_b?: unknown;
  sample_size?: number;
}

export interface OverlapResult {
  field_a: string;
  field_b: string;
  total_a: number;
  total_b: number;
  distinct_a: number;
  distinct_b: number;
  overlap: number;
  unique_to_a: number;
  unique_to_b: number;
  sample_overlap?: string[];
  warning?: string;
  elapsed_seconds: number;
}

interface OverlapError {
  error: string;
  count_a: number;
  count_b: number;
  suggestion: string;
}

function buildQuery(source: OverlapSource, filter?: unknown): Record<string, unknown> {
  const query: Record<string, unknown> = {
    'source-table': source.table_id,
  };
  if (filter) query.filter = filter;
  return query;
}

async function getCount(
  client: MetabaseClient,
  source: OverlapSource,
  filter: unknown | undefined,
  signal?: AbortSignal
): Promise<number> {
  const query = buildQuery(source, filter);
  query.aggregation = [['count']];
  const result = await client.queryDataset(source.database_id, query, signal);
  const count = result.rows[0]?.[0];
  if (count == null || typeof count !== 'number') {
    throw new Error(
      `Count query for db${source.database_id}.table${source.table_id} returned unexpected result: ${JSON.stringify(result.rows[0])}`
    );
  }
  return count;
}

async function fetchAllValues(
  client: MetabaseClient,
  source: OverlapSource,
  filter: unknown | undefined,
  totalCount: number,
  label: string,
  signal?: AbortSignal
): Promise<Set<string>> {
  const values = new Set<string>();
  let offset = 0;

  while (offset < totalCount) {
    signal?.throwIfAborted();

    const query: Record<string, unknown> = {
      'source-table': source.table_id,
      fields: [['field', source.field_id, null]],
      limit: BATCH_SIZE,
      offset,
    };
    if (filter) query.filter = filter;

    const result = await client.queryDataset(source.database_id, query, signal);

    for (const row of result.rows) {
      const val = row[0];
      if (val != null) values.add(String(val));
    }

    offset += BATCH_SIZE;

    if (offset % 10000 === 0 || offset >= totalCount) {
      const fetched = Math.min(offset, totalCount);
      console.error(`[cross_db_overlap] Fetched ${fetched.toLocaleString()}/${totalCount.toLocaleString()} from ${label}`);
    }

    if (result.row_count < BATCH_SIZE) break;
  }

  return values;
}

export async function crossDbOverlap(
  client: MetabaseClient,
  params: OverlapParams,
  signal?: AbortSignal
): Promise<OverlapResult | OverlapError> {
  const startTime = Date.now();
  const sampleSize = Math.min(params.sample_size || 0, 50);

  console.error('[cross_db_overlap] Counting rows on both sides...');
  const [countA, countB] = await Promise.all([
    getCount(client, params.source_a, params.filter_a, signal),
    getCount(client, params.source_b, params.filter_b, signal),
  ]);

  if (countA > MAX_ROWS_PER_SIDE || countB > MAX_ROWS_PER_SIDE) {
    const overSides: string[] = [];
    if (countA > MAX_ROWS_PER_SIDE) overSides.push(`Source A has ${countA.toLocaleString()} rows`);
    if (countB > MAX_ROWS_PER_SIDE) overSides.push(`Source B has ${countB.toLocaleString()} rows`);
    return {
      error: `${overSides.join(' and ')}, exceeding the ${MAX_ROWS_PER_SIDE.toLocaleString()} limit. Add filters to narrow the dataset(s).`,
      count_a: countA,
      count_b: countB,
      suggestion: 'Try filtering to active records, a specific date range, or a status filter.',
    };
  }

  console.error(`[cross_db_overlap] Source A: ${countA.toLocaleString()} rows, Source B: ${countB.toLocaleString()} rows. Fetching...`);

  const [setA, setB] = await Promise.all([
    fetchAllValues(client, params.source_a, params.filter_a, countA, 'source A', signal),
    fetchAllValues(client, params.source_b, params.filter_b, countB, 'source B', signal),
  ]);

  signal?.throwIfAborted();
  console.error('[cross_db_overlap] Computing intersection...');

  const overlapValues: string[] = [];
  const [smaller, larger] = setA.size <= setB.size ? [setA, setB] : [setB, setA];
  for (const val of smaller) {
    if (larger.has(val)) {
      overlapValues.push(val);
    }
  }

  const overlapCount = overlapValues.length;
  const elapsedSeconds = Math.round((Date.now() - startTime) / 1000);

  const result: OverlapResult = {
    field_a: `db${params.source_a.database_id}.table${params.source_a.table_id}.field${params.source_a.field_id}`,
    field_b: `db${params.source_b.database_id}.table${params.source_b.table_id}.field${params.source_b.field_id}`,
    total_a: countA,
    total_b: countB,
    distinct_a: setA.size,
    distinct_b: setB.size,
    overlap: overlapCount,
    unique_to_a: setA.size - overlapCount,
    unique_to_b: setB.size - overlapCount,
    elapsed_seconds: elapsedSeconds,
  };

  if (sampleSize > 0) {
    result.sample_overlap = overlapValues.slice(0, sampleSize);
  }

  if (countA > WARN_THRESHOLD || countB > WARN_THRESHOLD) {
    result.warning = `Large comparison — ${countA > WARN_THRESHOLD ? `source A has ${countA.toLocaleString()} rows` : ''}${countA > WARN_THRESHOLD && countB > WARN_THRESHOLD ? ', ' : ''}${countB > WARN_THRESHOLD ? `source B has ${countB.toLocaleString()} rows` : ''}`;
  }

  console.error(`[cross_db_overlap] Done in ${elapsedSeconds}s. Overlap: ${overlapCount.toLocaleString()}`);
  return result;
}
