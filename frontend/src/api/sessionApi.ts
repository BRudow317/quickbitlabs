import { tableFromIPC, tableFromArrays, tableToIPC } from 'apache-arrow';
import { client } from '@/api/openapi/client.gen';

// ── TypeScript mirrors of server/plugins/PluginModels.py ─────────────────────
// These are the universal contract types — no plugin-specific concepts here.

export interface Locator {
  plugin?: string | null;
  environment?: string | null;
  namespace?: string | null;
  entity_name?: string | null;
}

export interface Column {
  name: string;
  alias?: string | null;
  locator?: Locator | null;
  arrow_type_id?: string | null;
  primary_key?: boolean;
  is_nullable?: boolean;
  [key: string]: unknown;
}

export interface Entity {
  name: string;
  alias?: string | null;
  namespace?: string | null;
  plugin?: string | null;
  columns?: Column[];
  properties?: Record<string, unknown>;
}

// ── Filter / Assignment AST ───────────────────────────────────────────────────
// Mirrors PluginModels.py Operation / OperatorGroup / Assignment exactly.

export type OperatorLiteral =
  | "=" | "!=" | ">" | "<" | ">=" | "<="
  | "IN" | "NOT IN"
  | "LIKE" | "NOT LIKE"
  | "BETWEEN" | "NOT BETWEEN"
  | "IS NULL" | "IS NOT NULL"

export interface Operation {
  independent: Column;
  operator: OperatorLiteral;
  dependent?: string | unknown[] | Column | null;
}

export interface OperatorGroup {
  condition: "AND" | "OR" | "NOT";
  operation_group: Array<Operation | OperatorGroup>;
}

export interface Assignment {
  column: Column;
  value: string | unknown[] | Column | null;
}

// ── Join / Sort ───────────────────────────────────────────────────────────────
// Mirrors PluginModels.py Join / Sort exactly.

export interface Join {
  left_entity: Entity;
  left_column: Column;
  right_entity: Entity;
  right_column: Column;
  join_type: "INNER" | "LEFT" | "OUTER";
}

export interface Sort {
  column: Column;
  direction: "ASC" | "DESC";
  nulls_first?: boolean | null;
}

// ── Catalog ───────────────────────────────────────────────────────────────────

export interface Catalog {
  name?: string | null;
  entities?: Entity[];
  filters?: OperatorGroup[];
  assignments?: Assignment[];
  joins?: Join[];
  sort_columns?: Sort[];
  limit?: number | null;
  offset?: number | null;
  properties?: Record<string, unknown>;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
}

// ── Session ───────────────────────────────────────────────────────────────────

export async function getSession(): Promise<Catalog> {
  const res = await client.instance.get<Catalog>('/api/session/');
  return res.data;
}

export async function listSystems(): Promise<string[]> {
  const res = await client.instance.get<string[]>('/api/session/systems');
  return res.data;
}

// ── Data (Arrow IPC via federation) ──────────────────────────────────────────

export async function getData(catalog: Catalog): Promise<QueryResult> {
  const res = await client.instance.post<ArrayBuffer>('/api/data/', catalog, {
    responseType: 'arraybuffer',
  });
  return _decodeArrowIPC(res.data);
}

// ── Write operations (multipart: catalog_json + Arrow IPC file) ───────────────

export async function insertData(
  catalog: Catalog,
  rows: Record<string, unknown>[],
  cols: string[],
): Promise<QueryResult> {
  const res = await client.instance.put<ArrayBuffer>(
    '/api/data/insert',
    _makeWriteForm(catalog, rows, cols),
    { responseType: 'arraybuffer' },
  );
  return _decodeArrowIPC(res.data);
}

export async function upsertData(
  catalog: Catalog,
  rows: Record<string, unknown>[],
  cols: string[],
): Promise<QueryResult> {
  const res = await client.instance.put<ArrayBuffer>(
    '/api/data/',
    _makeWriteForm(catalog, rows, cols),
    { responseType: 'arraybuffer' },
  );
  return _decodeArrowIPC(res.data);
}

export async function updateData(
  catalog: Catalog,
  rows: Record<string, unknown>[],
  cols: string[],
): Promise<QueryResult> {
  const res = await client.instance.patch<ArrayBuffer>(
    '/api/data/',
    _makeWriteForm(catalog, rows, cols),
    { responseType: 'arraybuffer' },
  );
  return _decodeArrowIPC(res.data);
}

export async function deleteData(
  catalog: Catalog,
  rows: Record<string, unknown>[],
  cols: string[],
): Promise<void> {
  await client.instance.delete('/api/data/', {
    data: _makeWriteForm(catalog, rows, cols),
  });
}

// ── Internal ──────────────────────────────────────────────────────────────────

function _makeWriteForm(
  catalog: Catalog,
  rows: Record<string, unknown>[],
  cols: string[],
): FormData {
  const fd = new FormData();
  fd.append('catalog_json', JSON.stringify(catalog));
  fd.append('file', _rowsToArrowBlob(rows, cols), 'data.arrow');
  return fd;
}

function _rowsToArrowBlob(rows: Record<string, unknown>[], cols: string[]): Blob {
  // Coerce all values to strings so Arrow type-inference stays consistent
  const data: Record<string, string[]> = {};
  for (const col of cols) {
    data[col] = rows.map((r) => {
      const v = r[col];
      return v == null ? '' : String(v);
    });
  }
  const table = tableFromArrays(data);
  const bytes = tableToIPC(table, 'stream');
  // Arrow's tableToIPC always produces a plain ArrayBuffer; the TS type is overly broad.
  return new Blob([bytes.buffer as ArrayBuffer], { type: 'application/vnd.apache.arrow.stream' });
}

function _decodeArrowIPC(buffer: ArrayBuffer): QueryResult {
  const table = tableFromIPC(new Uint8Array(buffer));
  const columns = table.schema.fields.map((f) => f.name);
  const total = table.numRows;
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < total; i++) {
    const row: Record<string, unknown> = {};
    for (const col of columns) {
      row[col] = table.getChild(col)?.get(i) ?? null;
    }
    rows.push(row);
  }
  return { columns, rows, total };
}
