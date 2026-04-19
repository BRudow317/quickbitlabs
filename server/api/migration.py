from __future__ import annotations
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.plugins.PluginRegistry import list_plugins
from server.services.FullMigration import FullMigration

router = APIRouter()


class MigrationRequest(BaseModel):
    source_plugin: str
    target_plugin: str
    entities: list[str] | None = None
    source_catalog_name: str | None = None
    target_catalog_name: str | None = None


class MigrationEntityResult(BaseModel):
    entity: str
    target: str | None = None
    status: str
    message: str | None = None


class MigrationResult(BaseModel):
    results: list[MigrationEntityResult]
    succeeded: int
    failed: int


@router.get("/plugins", operation_id="list_migration_plugins")
def list_migration_plugins() -> list[str]:
    return list_plugins()


@router.post("/run", operation_id="run_migration", response_model=MigrationResult)
def run_migration(request: MigrationRequest) -> MigrationResult:
    try:
        migration = FullMigration(
            source_plugin=request.source_plugin,
            target_plugin=request.target_plugin,
            entities=request.entities,
            source_catalog_name=request.source_catalog_name,
            target_catalog_name=request.target_catalog_name,
        )
        raw_results = migration.run_all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    results = [
        MigrationEntityResult(
            entity=r.get("entity", ""),
            target=r.get("target"),
            status=r.get("status", "unknown"),
            message=r.get("message"),
        )
        for r in raw_results
    ]
    ok = sum(1 for r in results if r.status == "ok")
    return MigrationResult(results=results, succeeded=ok, failed=len(results) - ok)
