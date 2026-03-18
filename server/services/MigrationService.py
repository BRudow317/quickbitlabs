from __future__ import annotations
from typing import Any

from server.connectors.registry import get_connector
from server.models.ConnectorProtocol import Connector
from server.models.ConnectorStandard import Schema, Table, DataStream

import logging
logger = logging.getLogger(__name__)


class MigrationService:
    """
    Orchestrates data migration through the StandardTemplate contract.
    
    The Schema is the foundation — initialized first, populated by each stage.
    Source fills source_*, target fills target_*, data flows along
    the paths the Schema defines.

    The same Schema can be persisted, reloaded, and handed to any
    combination of connectors — the orchestration is built once,
    connectors are plugins.
    """
    source: Connector
    target: Connector
    schema: Schema

    def __init__(self, source_name: str, target_name: str, schema: Schema | None = None, **kwargs: Any):
        self.schema = schema or Schema(source_name=source_name, tables=[])
        self.source = get_connector(source_name, **kwargs.get('source_kwargs', {}))
        self.target = get_connector(target_name, **kwargs.get('target_kwargs', {}))

    def discover(self, streams: list[str] | None = None) -> Schema:
        """Source populates source_name at every level."""
        self.schema = self.source.get_schema(streams=streams) if streams else self.source.get_schema()
        logger.info(f"Discovered {len(self.schema.tables)} tables from {self.schema.source_name}")
        return self.schema

    def prepare(self) -> Schema:
        """Target populates target_name at every level, applies DDL."""
        self.schema = self.target.prepare_schema(self.schema)
        logger.info(f"Prepared {self.schema.target_name} ({len(self.schema.tables)} tables)")
        return self.schema

    def migrate_table(self, table: Table) -> None:
        """Source streams, target loads, for a single table."""
        records: DataStream = self.source.read_data(table.source_name)
        self.target.write_data(table.target_name or table.source_name, records)

    def run(self, streams: list[str] | None = None) -> Schema:
        """Full pipeline: discover -> prepare -> migrate."""
        assert self.source.test_connection(), "Source connection failed"
        assert self.target.test_connection(), "Target connection failed"

        self.discover(streams)
        self.prepare()

        failed: list[tuple[str, str]] = []
        for table in self.schema.tables:
            try:
                self.migrate_table(table)
                logger.info(f"Migrated {table.source_name} -> {table.target_name}")
            except Exception as e:
                failed.append((table.source_name, str(e)))
                logger.error(f"Failed {table.source_name}: {e}")

        logger.info(f"Complete: {len(self.schema.tables) - len(failed)}/{len(self.schema.tables)}")
        if failed:
            logger.warning(f"Failed tables: {failed}")
        return self.schema