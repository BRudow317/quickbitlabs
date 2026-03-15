"""
python master.py --config ./.env -l ./logs -v --exec python -m server.services.migration
"""
from __future__ import annotations

from server.connectors.sf.SalesforceConnector import SalesforceConnector
from server.connectors.postgres.PostgresConnector import PostgresConnector

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from server.models.StandardTemplate import Schema, Table

import logging
logger = logging.getLogger(__name__)

def run_migration():
    """POC for sf to pg"""
    logger.debug("Initializing Migration POC...")
    
    sf = SalesforceConnector()
    pg = PostgresConnector()

    # Test tables
    streams_to_migrate = ["Contact", "Account"]

    for stream in streams_to_migrate:
        logger.debug(f"\n--- Starting Migration for: {stream} ---")

        # Get the Salesforce schema converted to the generic interface
        logger.debug(f"Discovering schema for {stream}...")
        source_schema: Schema = sf.get_schema(streams=[stream])
        target_table: Table = source_schema.tables[0]

        # Passing schema to Postgres, SQLAlchemy generates and executes the DDL
        logger.debug(f"Generating and applying Postgres DDL for {stream}...")
        pg.apply_schema(target_table)

        # stream data SF -> yield a dict -> PG
        logger.debug(f"Extracting data from Salesforce for {stream}...")
        record_iterator = sf.read_data(stream)

        logger.debug(f"Loading data into Postgres for {stream}...")
        pg.write_data(stream_name=stream, records=record_iterator)

        logger.debug(f"Successfully migrated {stream}!")

if __name__ == "__main__":
    try:
        run_migration()
    except Exception as e:
        logger.error(f"Migration failed: {e}")