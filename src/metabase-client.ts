/**
 * Metabase REST API Client
 *
 * Wraps all HTTP interactions with a Metabase instance.
 * Designed for read-only access. Write protection is enforced at the SQL level
 * in runNativeQuery; MBQL queries go through Metabase's own permission system.
 *
 * Authentication: Uses x-api-key header (set via METABASE_API_KEY env var)
 * Cancellation: All methods accept an optional AbortSignal for clean cancellation
 *
 * @module metabase-client
 */

export class MetabaseClient {
  private apiKey: string;
  private baseUrl: string;

  /**
   * Create a new MetabaseClient instance.
   *
   * @param baseUrl - The Metabase API base URL (e.g. https://metabase.example.com/api).
   * @param apiKey  - The Metabase API key.
   */
  constructor(baseUrl: string, apiKey: string) {
    if (!baseUrl) {
      throw new Error('Metabase base URL is required. Set METABASE_URL env var.');
    }
    if (!apiKey) {
      throw new Error('Metabase API key is required. Set METABASE_API_KEY env var.');
    }
    this.baseUrl = baseUrl;
    this.apiKey = apiKey;
  }

  /**
   * Generic HTTP request handler for all Metabase API calls.
   *
   * Builds the full URL from the instance's baseUrl and the given path,
   * attaches the x-api-key authentication header, and handles JSON
   * serialisation and error reporting.
   */
  private async request<T>(
    method: string,
    path: string,
    body?: Record<string, unknown>,
    signal?: AbortSignal
  ): Promise<T> {
    const url = `${this.baseUrl}${path}`;

    const headers: Record<string, string> = {
      'x-api-key': this.apiKey,
      'Accept': 'application/json',
    };

    // Combine caller's abort signal with a 30-second per-request timeout
    const timeoutMs = 30_000;
    const timeoutSignal = AbortSignal.timeout(timeoutMs);
    const combinedSignal = signal
      ? AbortSignal.any([signal, timeoutSignal])
      : timeoutSignal;

    const options: RequestInit = { method, headers, signal: combinedSignal };

    if (body && (method === 'POST' || method === 'PUT')) {
      headers['Content-Type'] = 'application/json';
      options.body = JSON.stringify(body);
    }

    const response = await fetch(url, options);

    if (!response.ok) {
      let errorBody = '';
      try {
        const fullBody = await response.text();
        errorBody = fullBody.slice(0, 2000);
      } catch {
        errorBody = '(unable to read error response body)';
      }
      throw new Error(`Metabase API error ${response.status}: ${errorBody}`);
    }

    const text = await response.text();
    if (!text) {
      throw new Error(`Metabase API returned empty response (${response.status}) for ${method} ${path}`);
    }
    try {
      return JSON.parse(text) as T;
    } catch (parseErr) {
      throw new Error(
        `Metabase returned non-JSON response (${response.status}): ${text.slice(0, 200)}. ` +
        `Parse error: ${parseErr instanceof Error ? parseErr.message : String(parseErr)}`
      );
    }
  }

  // ── Discovery Methods ─────────────────────────────────────────────────

  async listDatabases(signal?: AbortSignal) {
    const raw = await this.request<{ data: Array<{ id: number; name: string; engine: string }> }>(
      'GET', '/database', undefined, signal
    );
    const dbs = Array.isArray(raw) ? raw : raw.data;
    if (!Array.isArray(dbs)) {
      throw new Error(`Unexpected response from /database endpoint. Expected array or { data: [...] }.`);
    }
    return dbs.map((db) => ({ id: db.id, name: db.name, engine: db.engine }));
  }

  async listTables(databaseId: number, signal?: AbortSignal) {
    const raw = await this.request<{ tables: Array<{ id: number; name: string; schema: string }> }>(
      'GET', `/database/${databaseId}/metadata?include_hidden=true`, undefined, signal
    );
    if (!raw.tables) {
      throw new Error(`Unexpected response for database ${databaseId} metadata: missing 'tables' key.`);
    }
    return raw.tables.map((t) => ({ id: t.id, name: t.name, schema: t.schema }));
  }

  async getTableFields(tableId: number, signal?: AbortSignal) {
    const raw = await this.request<{ fields: Array<{ id: number; name: string; base_type: string; database_type: string; semantic_type: string | null }> }>(
      'GET', `/table/${tableId}/query_metadata`, undefined, signal
    );
    if (!raw.fields) {
      throw new Error(`Unexpected response for table ${tableId} metadata: missing 'fields' key.`);
    }
    return raw.fields.map((f) => ({
      id: f.id,
      name: f.name,
      base_type: f.base_type,
      database_type: f.database_type,
      semantic_type: f.semantic_type,
    }));
  }

  // ── Querying Methods ──────────────────────────────────────────────────

  async queryDataset(
    databaseId: number,
    query: Record<string, unknown>,
    signal?: AbortSignal
  ) {
    const raw = await this.request<{
      data: {
        cols: Array<{ name: string }>;
        rows: unknown[][];
      };
    }>('POST', '/dataset', { database: databaseId, type: 'query', query }, signal);

    if (!raw.data) {
      throw new Error(`Metabase query returned no data. The query may have failed or the database may be unreachable.`);
    }
    const cols = (raw.data.cols || []).map((c) => c.name);
    const rows = raw.data.rows || [];
    return { columns: cols, rows, row_count: rows.length };
  }

  /**
   * Execute a native SQL query against a database.
   *
   * Requires that the API key has native query (SQL) permission in Metabase.
   * The caller is responsible for including LIMIT in the SQL.
   */
  async runNativeQuery(
    databaseId: number,
    query: string,
    signal?: AbortSignal
  ) {
    // Write-operation guard — this server is read-only.
    // This is a defense-in-depth safeguard; Metabase's own permission system
    // is the primary access control. This check catches common cases.
    const stripped = query
      .replace(/--[^\n]*/g, '')          // strip single-line comments
      .replace(/\/\*[\s\S]*?\*\//g, '')  // strip block comments
      .trim()
      .toUpperCase();

    // Reject multi-statement queries (semicolon followed by another statement)
    if (/;\s*\S/.test(stripped)) {
      throw new Error('Multi-statement queries are not allowed. This server is read-only.');
    }

    const forbidden = [
      'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'TRUNCATE',
      'CREATE', 'GRANT', 'REVOKE', 'EXEC', 'CALL', 'MERGE',
      'UPSERT', 'REPLACE',
    ];
    // Check both the start of the query and full-body keyword search
    const words = stripped.split(/\s+/);
    for (const keyword of forbidden) {
      if (words[0] === keyword || words.includes(keyword)) {
        throw new Error(`Write operations are not allowed. This server is read-only. Found: ${keyword}`);
      }
    }

    const raw = await this.request<{
      data: {
        cols: Array<{ name: string }>;
        rows: unknown[][];
      };
    }>('POST', '/dataset', {
      database: databaseId,
      type: 'native',
      native: { query },
    }, signal);

    if (!raw.data) {
      throw new Error(`Metabase native query returned no data. Check SQL syntax and database permissions.`);
    }
    const cols = (raw.data.cols || []).map((c) => c.name);
    const rows = raw.data.rows || [];
    return { columns: cols, rows, row_count: rows.length };
  }

  async runSavedQuestion(cardId: number, signal?: AbortSignal) {
    const raw = await this.request<{
      data: {
        cols: Array<{ name: string }>;
        rows: unknown[][];
      };
    }>('POST', `/card/${cardId}/query`, undefined, signal);

    if (!raw.data) {
      throw new Error(`Saved question ${cardId} returned no data. It may have failed or you may lack permission.`);
    }
    const cols = (raw.data.cols || []).map((c) => c.name);
    const rows = raw.data.rows || [];
    return { columns: cols, rows, row_count: rows.length };
  }

  async getCardMetadata(cardId: number, signal?: AbortSignal) {
    const raw = await this.request<{
      id: number;
      name: string;
      database_id: number;
      dataset_query: { type: string; query?: unknown; native?: { query: string } };
    }>('GET', `/card/${cardId}`, undefined, signal);

    if (!raw.dataset_query?.type) {
      throw new Error(`Card ${cardId} has no dataset_query type. It may not be a standard question card.`);
    }

    return {
      id: raw.id,
      name: raw.name,
      database_id: raw.database_id,
      query_type: raw.dataset_query.type,
      query_definition: raw.dataset_query.type === 'native'
        ? raw.dataset_query.native
        : raw.dataset_query.query,
    };
  }

  async listCards(signal?: AbortSignal) {
    const raw = await this.request<Array<{
      id: number;
      name: string;
      database_id: number;
      dataset_query: { type: string };
    }>>('GET', '/card', undefined, signal);

    if (!Array.isArray(raw)) {
      throw new Error(`Unexpected response from /card endpoint. Expected array.`);
    }
    return raw.map((c) => ({
      id: c.id,
      name: c.name,
      database_id: c.database_id,
      query_type: c.dataset_query?.type || 'unknown',
    }));
  }

  // ── Dashboard Methods ────────────────────────────────────────────────

  async listDashboards(signal?: AbortSignal) {
    const raw = await this.request<Array<{ id: number; name: string }>>(
      'GET', '/dashboard', undefined, signal
    );
    if (!Array.isArray(raw)) {
      throw new Error(`Unexpected response from /dashboard endpoint. Expected array.`);
    }
    return raw.map((d) => ({ id: d.id, name: d.name }));
  }

  async getDashboard(dashboardId: number, signal?: AbortSignal) {
    const raw = await this.request<{
      id: number;
      name: string;
      dashcards: Array<{
        id: number;
        card: { id: number; name: string } | null;
        size_x: number;
        size_y: number;
      }>;
    }>('GET', `/dashboard/${dashboardId}`, undefined, signal);

    return {
      id: raw.id,
      name: raw.name,
      cards: (raw.dashcards || [])
        .filter((dc): dc is typeof dc & { card: NonNullable<typeof dc.card> } => dc.card != null)
        .map((dc) => ({
          id: dc.id,
          name: dc.card.name,
          card_id: dc.card.id,
          size_x: dc.size_x,
          size_y: dc.size_y,
        })),
    };
  }

  // ── Search Methods ────────────────────────────────────────────────────

  async search(query: string, type?: string, signal?: AbortSignal) {
    const params = new URLSearchParams({ q: query });
    if (type) params.set('models', type);
    const raw = await this.request<{ data: Array<{ id: number; name: string; model: string; database_id: number }> }>(
      'GET', `/search?${params.toString()}`, undefined, signal
    );
    const results = Array.isArray(raw) ? raw : raw.data;
    if (!Array.isArray(results)) {
      throw new Error(`Unexpected response from /search endpoint. Expected array or { data: [...] }.`);
    }
    return results.map((r: { id: number; name: string; model: string; database_id: number }) => ({
      id: r.id,
      name: r.name,
      type: r.model,
      database_id: r.database_id,
    }));
  }
}
