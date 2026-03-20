from __future__ import annotations
from typing import Any

from server.connectors.registry import get_connector
from server.models.ConnectorProtocol import Connector
from server.models.ConnectorStandard import Schema, Table, DataStream

import logging
logger = logging.getLogger(__name__)


class MigrationService:
    """
    Orchestrates data migration through the Connector protocol.

    Source fills source_*, target fills target_*, data flows along
    the paths the Schema defines. No source- or target-specific logic
    lives here — connectors are plugins.

    Usage:
        migration = MigrationService(
            source_name='salesforce',
            target_name='postgres',
            target_schema='quickbitlabs',
            source_kwargs={...},
            target_kwargs={...},
        )
        schema = migration.run(streams=['Account', 'Contact', 'Opportunity'])
    """
    source: Connector
    target: Connector
    schema: Schema
    target_schema_name: str | None

    def __init__(
        self,
        source_name: str,
        target_name: str,
        schema: Schema | None = None,
        target_schema_name: str | None = None,
        **kwargs: Any,
    ):
        self.target_schema_name = target_schema_name
        self.schema = schema or Schema(source_name=source_name)
        self.source = get_connector(source_name, **kwargs.get('source_kwargs', {}))
        self.target = get_connector(target_name, **kwargs.get('target_kwargs', {}))

    #  Stages 

    def discover(self, streams: list[str] | None = None) -> Schema:
        """Source describes its objects and populates source_* fields on the Schema."""
        result = self.source.get_schema(streams=streams)
        if not result.ok or not result.data:
            raise RuntimeError(f"Discovery failed [{result.code}]: {result.message}")
        self.schema = result.data
        logger.info(f"Discovered {len(self.schema.tables)} tables from {self.schema.source_name}")
        return self.schema

    def prepare(self) -> Schema:
        """Stamp target_* fields onto the Schema, create the target schema, and apply DDL per table."""
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
                f"Schema creation failed [{schema_result.code}]: {schema_result.message}"
            )

        # Create each table (idempotent — CREATE TABLE IF NOT EXISTS)
        table_failures: list[tuple[str, str]] = []
        for table in self.schema.tables:
            result = self.target.create_table(table, pg_schema=self.schema.target_name)
            if not result.ok:
                table_failures.append((table.source_name, result.message))
                logger.warning(f"Could not create table {table.source_name}: {result.message}")

        ready = len(self.schema.tables) - len(table_failures)
        logger.info(f"Prepared '{self.schema.target_name}': {ready}/{len(self.schema.tables)} tables ready")
        if table_failures:
            logger.warning(f"Table creation failures: {table_failures}")

        return self.schema

    def migrate_table(self, table: Table) -> None:
        """Stream records from source and upsert into target for a single table."""
        read_result = self.source.get_records(table)
        if not read_result.ok or read_result.data is None:
            raise RuntimeError(
                f"get_records failed [{read_result.code}]: {read_result.message}"
            )

        write_result = self.target.upsert_records(
            table,
            read_result.data,
            pg_schema=self.schema.target_name,
        )
        if not write_result.ok:
            raise RuntimeError(
                f"upsert_records failed [{write_result.code}]: {write_result.message}"
            )

    def run(self, streams: list[str] | None = None) -> Schema:
        """Full pipeline: discover -> prepare -> migrate all tables."""
        if not self.source.test_connection():
            raise RuntimeError("Source connection failed")
        if not self.target.test_connection():
            raise RuntimeError("Target connection failed")

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
