import { client } from '@/api/openapi/client.gen';

export interface MigrationEntityResult {
  entity: string;
  target: string | null;
  status: string;
  message: string | null;
}

export interface MigrationResult {
  results: MigrationEntityResult[];
  succeeded: number;
  failed: number;
}

export interface MigrationRequest {
  source_plugin: string;
  target_plugin: string;
  entities?: string[] | null;
}

export async function runMigration(req: MigrationRequest): Promise<MigrationResult> {
  const res = await client.instance.post<MigrationResult>('/api/migration/run', req);
  return res.data;
}
