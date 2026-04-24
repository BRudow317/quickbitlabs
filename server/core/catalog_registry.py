from __future__ import annotations

import logging
from typing import Any

from oracledb import DB_TYPE_CLOB

from server.db.ServerDatabase import ServerDatabase
from server.plugins.PluginModels import Catalog



logger = logging.getLogger(__name__)

class CatalogRegistryService:
    """
    Persists named Catalog snapshots to CATALOG_REGISTRY table.

    Ownership is tracked by `owner` — the Oracle username stored in the JWT sub
    claim. The service account (ServerDatabase) performs the actual DB operations;
    no user password is required after login.
    """
    
    _TABLE = "CATALOG_REGISTRY"
    from server.db.db import server_db
    _server_db: ServerDatabase = server_db

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def save(self, owner: str, registry_key: str, catalog: Catalog) -> None:
        """Upsert a named catalog for owner. Creates or overwrites the existing entry."""
        json_str = catalog.model_dump_json()
        sql = """
            MERGE INTO CATALOG_REGISTRY cr
            USING DUAL
               ON (cr.OWNER = :owner AND cr.REGISTRY_KEY = :registry_key)
            WHEN MATCHED THEN
                UPDATE SET CATALOG_JSON = :catalog_json,
                           UPDATED_AT   = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (REGISTRY_KEY, OWNER, CATALOG_JSON, CREATED_AT, UPDATED_AT)
                VALUES (:registry_key, :owner, :catalog_json,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """
        with self._server_db.connect().cursor() as cur:
            # Force CLOB binding — catalog JSON can exceed oracledb's 32 KB direct-string limit
            cur.setinputsizes(catalog_json=DB_TYPE_CLOB)
            cur.execute(sql, owner=owner, registry_key=registry_key, catalog_json=json_str)
        logger.info(f"CatalogRegistry: saved '{registry_key}' for owner '{owner}'")

    def list_entries(self, owner: str) -> list[dict[str, Any]]:
        """Return lightweight metadata for all catalogs saved by owner (no JSON body)."""
        sql = """
            SELECT REGISTRY_KEY, CREATED_AT, UPDATED_AT
              FROM CATALOG_REGISTRY
             WHERE OWNER = :owner
             ORDER BY UPDATED_AT DESC
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, owner=owner)
            rows = cur.fetchall()
        return [
            {
                "registry_key": row[0],
                "created_at":   row[1].isoformat() if row[1] else None,
                "updated_at":   row[2].isoformat() if row[2] else None,
            }
            for row in rows
        ]

    def get(self, owner: str, registry_key: str) -> Catalog | None:
        """Retrieve a saved Catalog by owner + key. Returns None if not found."""
        sql = """
            SELECT CATALOG_JSON
              FROM CATALOG_REGISTRY
             WHERE OWNER = :owner
               AND REGISTRY_KEY = :registry_key
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, owner=owner, registry_key=registry_key)
            row = cur.fetchone()
        if row is None:
            return None
        json_str: str = row[0].read() if hasattr(row[0], "read") else row[0]
        return Catalog.model_validate_json(json_str)

    def delete(self, owner: str, registry_key: str) -> bool:
        """Delete a saved catalog. Returns True if a row was removed, False if not found."""
        sql = """
            DELETE FROM CATALOG_REGISTRY
             WHERE OWNER = :owner
               AND REGISTRY_KEY = :registry_key
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, owner=owner, registry_key=registry_key)
            deleted = cur.rowcount > 0
        logger.info(
            f"CatalogRegistry: {'deleted' if deleted else 'not found'} "
            f"'{registry_key}' for owner '{owner}'"
        )
        return deleted
