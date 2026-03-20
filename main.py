from __future__ import annotations

import logging
import sys
logger = logging.getLogger(__name__)

from server.services.MigrationService import MigrationService

migration = MigrationService(
    source_name="salesforce",
    target_name="postgres",
    target_schema_name="quickbitlabs",
)

streams = sys.argv[1:] or None  # e.g. python main.py Account Contact Opportunity

schema = migration.run(streams=streams)
