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
    2. Creates a named view per entity, normalising whatever column naming the plugin used
       (simple "Col" or qualified "Entity.Col") to simple names so the master SQL can use
       table.column dot-notation against the view names.
    3. Builds and executes the master SQL query.
    """
    con = duckdb.connect()

    for i, (child, reader) in enumerate(children_streams):
        # Read schema metadata before handing the reader to DuckDB — schema access is non-consuming.
        actual_cols: set[str] = set(reader.schema.names)
        stream_name = f"stream_{i}"
        con.register(stream_name, reader)

        for entity in child.entities:
            proj: list[str] = []
            for c in entity.columns:
                qual = f"{entity.name}.{c.name}"
                if qual in actual_cols:
                    # Plugin returned qualified names — alias back to simple.
                    proj.append(f'"{qual}" AS "{c.name}"')
                elif c.name in actual_cols:
                    proj.append(f'"{c.name}"')
                # Columns absent from the stream are silently skipped.
            if proj:
                con.execute(f'CREATE VIEW "{entity.name}" AS SELECT {", ".join(proj)} FROM {stream_name}')
            else:
                # Fallback: expose the whole stream — master query may still filter it down.
                con.execute(f'CREATE VIEW "{entity.name}" AS SELECT * FROM {stream_name}')

    sql, binds = build_duckdb_select(catalog)
    arrow_reader: ArrowReader = con.execute(sql, binds).to_arrow_reader()
    return arrow_reader

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
    Collects successes and failures independently - partial failure returns 207.
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