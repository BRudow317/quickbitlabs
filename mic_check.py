"""
python master.py --config ./.env -l ./logs --exec server/mic_check.py
"""
from __future__ import annotations
import sys

from server.connectors.supabase.destination import PostgresDestination

import logging
logger = logging.getLogger(__name__)

def main():
    try:
        mic_check = PostgresDestination()
        if mic_check.test_connection(): 
            logger.info("Mic check passed!")
            return 0
        else: raise Exception("Mic check failed!")
    except Exception as e:
        logger.error(f"Mic check failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()