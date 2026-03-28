#!/usr/bin/env python3
"""
Oracle -> Postgres migration script.

Usage:
    python test_migration_oracle.py                 # migrate all tables
    python test_migration_oracle.py ORDERS PRODUCTS  # migrate named tables only
"""
from __future__ import annotations
import logging
import sys

logger = logging.getLogger(__name__)

from server.services.MigrationService import MigrationService as MS


def main() -> None:
    streams = sys.argv[1:] or None
    logger.debug('Starting Oracle -> Postgres migration...')
    migration = MS(
        source_name='oracle',
        target_name='postgres',
        target_schema_name='quickbitlabs',
    )
    schema = migration.run(streams=streams)
    logger.debug(
        f"Migration completed. {len(schema.tables)} table(s) migrated "
        f"into '{schema.target_name}'."
    )


if __name__ == '__main__':
    main()
