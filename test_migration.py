from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
from server.services.MigrationService import MigrationService as MS
def main():
    import sys
    streams = sys.argv[1:] or None
    logger.debug("Starting migration...")
    migration = MS(
        source_name='salesforce', 
        target_name='postgres', 
        target_schema_name='quickbitlabs')
    schema = migration.run(streams=streams)
    logger.debug(f"Migration completed. {len(schema.tables)} tables in '{schema.target_name}'.")
if __name__ == "__main__":
    main()
