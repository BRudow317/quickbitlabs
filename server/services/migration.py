from __future__ import annotations

# 1. Strict imports: ONLY the connectors are allowed.
from server.connectors.sf.SalesforceConnector import SalesforceConnector
from server.connectors.postgres.PostgresConnector import PostgresConnector

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from server.models.StandardTemplate import Schema, Table

import logging
logger = logging.getLogger(__name__)

def run_migration():
    """POC for sf to pg"""
    logger.info("Initializing Migration POC...")
    
    sf = SalesforceConnector()
    pg = PostgresConnector()

    # Test tables
    streams_to_migrate = ["Contact", "Account"]

    for stream in streams_to_migrate:
        logger.info(f"\n--- Starting Migration for: {stream} ---")

        # Get the Salesforce schema converted to the generic interface
        logger.info(f"Discovering schema for {stream}...")
        source_schema: Schema = sf.get_schema(streams=[stream])
        target_table: Table = source_schema.tables[0]

        # Passing schema to Postgres, SQLAlchemy generates and executes the DDL
        logger.info(f"Generating and applying Postgres DDL for {stream}...")
        pg.apply_schema(target_table)

        # stream data SF -> yield a dict -> PG
        logger.info(f"Extracting data from Salesforce for {stream}...")
        record_iterator = sf.read_data(stream)

        logger.info(f"Loading data into Postgres for {stream}...")
        pg.write_data(stream_name=stream, records=record_iterator)

        logger.info(f"Successfully migrated {stream}!")

if __name__ == "__main__":
    run_migration()