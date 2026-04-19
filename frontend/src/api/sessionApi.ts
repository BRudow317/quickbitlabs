import { tableFromIPC } from 'apache-arrow';
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
  columns?: Column[];
  properties?: Record<string, unknown>;
}

export interface Catalog {
  name?: string | null;
  entities?: Entity[];
  limit?: number | null;
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

// ── Internal ──────────────────────────────────────────────────────────────────

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
