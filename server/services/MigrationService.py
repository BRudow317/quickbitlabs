from __future__ import annotations
from typing import Any

from server.connectors.registry import get_connector
from server.models.PluginProtocol import Connector
from server.models.ConnectorStandard import BaseBaseSchema, BaseBaseTable, DataStream

import logging
logger = logging.getLogger(__name__)


class MigrationService:
    source: Connector
    target: Connector
    schema: BaseSchema
    target_schema_name: str | None

    def __init__(
        self,
        source_name: str,
        target_name: str,
        schema: BaseSchema | None = None,
        target_schema_name: str | None = None,
        **kwargs: Any,
    ):
        self.target_schema_name = target_schema_name
        self.schema = schema or BaseSchema(source_name=source_name)
        self.source = get_connector(source_name, **kwargs.get('source_kwargs', {}))
        self.target = get_connector(target_name, **kwargs.get('target_kwargs', {}))

    # Stages 
    def discover(self, streams: list[str] | None = None) -> BaseSchema:
        """Source describes its objects and populates source_* fields on the BaseSchema."""
        result = self.source.get_schema(streams=streams)
        if not result.ok or not result.data:
            raise RuntimeError(f"Discovery failed [{result.code}]: {result.message}")
        self.schema = result.data
        logger.info(f"Discovered {len(self.schema.tables)} tables from {self.schema.source_name}")
        return self.schema

    def prepare(self) -> BaseSchema:
        """Stamp target_* fields onto the BaseSchema, create the target schema, and apply DDL per table."""
        # Stamp target names — source and target names match unless explicitly remapped
        self.schema.target_name = self.target_schema_name or self.schema.source_name
        for table in self.schema.tables:
            table.target_name = table.target_name or table.source_name
            for col in table.columns:
                col.target_name = col.target_name or col.source_name

        # Create target schema (idempotent)
        schema_result = self.target.upsert_schema(self.schema.target_name)
        if not schema_result.ok:
            raise RuntimeError(
                f"BaseSchema creation failed [{schema_result.code}]: {schema_result.message}"
            )

        # Upsert each table (idempotent — create if absent, alter if present, stamps target_name)
        table_failures: list[tuple[str, str]] = []
        for table in self.schema.tables:
            result = self.target.upsert_table(table, owner=self.schema.target_name)
            if not result.ok:
                table_failures.append((table.source_name, result.message))
                logger.warning(f"Could not create table {table.source_name}: {result.message}")

        ready = len(self.schema.tables) - len(table_failures)
        logger.info(f"Prepared '{self.schema.target_name}': {ready}/{len(self.schema.tables)} tables ready")
        if table_failures:
            logger.warning(f"BaseTable creation failures: {table_failures}")

        return self.schema

    def migrate_table(self, table: BaseTable) -> None:
        read_result = self.source.get_records(table)
        if not read_result.ok or read_result.data is None:
            raise RuntimeError(
                f"get_records failed [{read_result.code}]: {read_result.message}"
            )

        write_result = self.target.upsert_records(
            table,
            read_result.data,
            owner=self.schema.target_name,
        )
        if not write_result.ok:
            raise RuntimeError(
                f"upsert_records failed [{write_result.code}]: {write_result.message}"
            )

    def run(self, streams: list[str] | None = None) -> BaseSchema:
        if not self.source.test_connection(): raise RuntimeError("Source connection failed")
        if not self.target.test_connection(): raise RuntimeError("Target connection failed")

        self.discover(streams)
        self.prepare()

        failed: list[tuple[str, str]] = []
        for table in self.schema.tables:
            try:
                self.migrate_table(table)
                logger.info(
                    f"Migrated {table.source_name} -> {table.target_name or table.source_name}"
                )
            except Exception as e:
                failed.append((table.source_name, str(e)))
                logger.error(f"Failed {table.source_name}: {e}")

        total = len(self.schema.tables)
        logger.info(f"Complete: {total - len(failed)}/{total} tables migrated into '{self.schema.target_name}'")
        if failed:
            logger.warning(f"Failed tables: {[name for name, _ in failed]}")

        return self.schema
