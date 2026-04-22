import duckdb
from fastapi.responses import JSONResponse
from typing import Any, cast, TYPE_CHECKING
from server.plugins.PluginModels import Catalog, ArrowReader
from server.plugins.PluginRegistry import get_plugin, PLUGIN
from server.core.DuckDBDialect import build_duckdb_select

if TYPE_CHECKING:
    from server.plugins.PluginProtocol import Plugin

def duckdb_orchestrator(catalog: Catalog, children_streams: list[tuple[Catalog, ArrowReader]]) -> ArrowReader:
    """
    Executes a federated join across multiple plugin streams using DuckDB.
    1. Registers each child stream as a unique temporary table.
    2. Creates views for each entity to map 'Entity.Column' names back to 'Column'.
    3. Builds and executes the master SQL query.
    """
    con = duckdb.connect()
    
    # 1. Register streams and create views
    for i, (child, reader) in enumerate(children_streams):
        stream_name = f"stream_{i}"
        con.register(stream_name, reader)
        
        for entity in child.entities:
            # Map the qualified names (e.g. "Table.Column") from the plugin stream back to simple names
            cols = [f'"{entity.name}.{c.name}" AS "{c.name}"' for c in entity.columns]
            con.execute(f'CREATE VIEW "{entity.name}" AS SELECT {", ".join(cols)} FROM {stream_name}')
        
    # 2. Build and execute master query
    sql, binds = build_duckdb_select(catalog)
    return con.execute(sql, binds).fetch_record_batch_reader()

def execute_federation(master_catalog: Catalog) -> dict[str, tuple[Plugin, Catalog]]:
    """
    Federates a master catalog into per-system (plugin, child_catalog) pairs.
    Used by the data router for the federated DuckDB join path.
    """
    children: list[Catalog] = master_catalog.federate
    return {
        child.source_type: (get_plugin(cast(PLUGIN, child.source_type)), child)
        for child in children
        if child.source_type
    }

def fanout(catalog: Catalog, method: str) -> JSONResponse:
    """
    Federates a catalog and fans out a single DDL method across all child systems.
    Collects successes and failures independently — partial failure returns 207.
    Used by catalog, entity, and column DDL routers.
    """
    children: list[Catalog] = catalog.federate
    succeeded: dict[str, Any] = {}
    failed: dict[str, str] = {}

    for child in children:
        if not child.source_type:
            failed["unknown"] = "Child catalog is missing source_type"
            continue
        plugin: Plugin = get_plugin(cast(PLUGIN, child.source_type))
        resp = getattr(plugin, method)(child)
        if resp.ok:
            succeeded[child.source_type] = resp.data.model_dump(mode="json") if resp.data else None
        else:
            failed[child.source_type] = resp.message

    status_code = 200 if not failed else (207 if succeeded else 500)
    return JSONResponse(
        status_code=status_code,
        content={"succeeded": succeeded, "failed": failed}
    )