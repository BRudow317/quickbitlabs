from __future__ import annotations

import json
import logging
from typing import Any

import oracledb
from oracledb import DB_TYPE_CLOB

from server.db.ServerDatabase import ServerDatabase
from server.plugins.PluginModels import Catalog

logger = logging.getLogger(__name__)

# Catalog list fields stored as individual JSON CLOB columns.
_JSON_FIELDS = ("entities", "filters", "joins", "sort_columns", "assignments", "properties")


class CatalogRegistryService:
    """
    Persists Catalog snapshots in QBL_CATALOG_REGISTRY.

    SYSTEM catalogs have owner_user_id IS NULL.
    User catalogs have owner_user_id set to the user's qbl_user_id.
    The unique discriminator within an owner's scope is catalog.name.
    """

    from server.db.db import server_db
    _server_db: ServerDatabase = server_db

    # ================================================
    # Internal helpers
    # ================================================

    def _get_user_id(self, owner: str) -> int | None:
        if owner == "SYSTEM":
            return None
        sql = "SELECT qbl_user_id FROM QBL_USERS WHERE USERNAME = :u"
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, u=owner)
            row = cur.fetchone()
        return int(row[0]) if row else None

    def _get_catalog_id(self, owner_user_id: int | None, name: str) -> str | None:
        """Return catalog_id (VARCHAR2 UUID) for the given owner + name, or None."""
        sql = """
            SELECT catalog_id FROM QBL_CATALOG_REGISTRY
             WHERE name = :name
               AND (:user_id IS NULL AND owner_user_id IS NULL
                    OR owner_user_id = :user_id)
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, name=name, user_id=owner_user_id)
            row = cur.fetchone()
        return str(row[0]) if row else None

    def _dump_json_fields(self, catalog: Catalog) -> dict[str, str | None]:
        data = catalog.model_dump(mode="json")
        result: dict[str, str | None] = {}
        for field in _JSON_FIELDS:
            val = data.get(field)
            result[field] = json.dumps(val) if val is not None else None
        return result

    # ================================================
    # CRUD
    # ================================================

    def save(self, owner: str, catalog: Catalog) -> None:
        """Upsert a catalog for owner using catalog.name as the unique key."""
        if not catalog.name:
            raise ValueError("catalog.name is required to save to the registry")

        user_id    = self._get_user_id(owner)
        con        = self._server_db.connect()
        catalog_id = self._get_catalog_id(user_id, catalog.name)
        jf         = self._dump_json_fields(catalog)

        _clob_fields = ("entities", "filters", "joins", "sort_columns", "assignments", "properties")

        if catalog_id is None:
            sql = """
                INSERT INTO QBL_CATALOG_REGISTRY
                    (owner_user_id,
                     name, alias, label, namespace, catalog_version,
                     description, source_type, catalog_limit, catalog_offset,
                     entities, filters, joins, sort_columns, assignments, properties,
                     created_at, created_by, updated_at, updated_by)
                VALUES
                    (:user_id,
                     :name, :alias, :label, :namespace, :cat_version,
                     :description, :source_type, :cat_limit, :cat_offset,
                     :entities, :filters, :joins, :sort_columns, :assignments, :properties,
                     SYSTIMESTAMP, :owner, SYSTIMESTAMP, :owner)
            """
            with con.cursor() as cur:
                cur.setinputsizes(**{f: DB_TYPE_CLOB for f in _clob_fields})
                cur.execute(sql, {
                    "user_id": user_id,
                    "name": catalog.name, "alias": catalog.alias,
                    "label": catalog.label,
                    "namespace": catalog.namespace, "cat_version": catalog.catalog_version,
                    "description": catalog.description, "source_type": catalog.source_type,
                    "cat_limit": catalog.catalog_limit, "cat_offset": catalog.catalog_offset,
                    "owner": owner,
                    **jf,
                })
                con.commit()
        else:
            sql = """
                UPDATE QBL_CATALOG_REGISTRY
                   SET alias           = :alias,
                       label           = :label,
                       namespace       = :namespace,
                       catalog_version = :cat_version,
                       description     = :description,
                       source_type     = :source_type,
                       catalog_limit   = :cat_limit,
                       catalog_offset  = :cat_offset,
                       entities        = :entities,
                       filters         = :filters,
                       joins           = :joins,
                       sort_columns    = :sort_columns,
                       assignments     = :assignments,
                       properties      = :properties,
                       updated_at      = SYSTIMESTAMP,
                       updated_by      = :owner
                 WHERE catalog_id = :catalog_id
            """
            with con.cursor() as cur:
                cur.setinputsizes(**{f: DB_TYPE_CLOB for f in _clob_fields})
                cur.execute(sql, {
                    "catalog_id": catalog_id,
                    "alias": catalog.alias,
                    "label": catalog.label,
                    "namespace": catalog.namespace, "cat_version": catalog.catalog_version,
                    "description": catalog.description, "source_type": catalog.source_type,
                    "cat_limit": catalog.catalog_limit, "cat_offset": catalog.catalog_offset,
                    "owner": owner,
                    **jf,
                })
                con.commit()

        logger.info("CatalogRegistry: saved '%s' for owner '%s'", catalog.name, owner)

    def list_entries(self, owner: str) -> list[dict[str, Any]]:
        """Return lightweight metadata for all catalogs saved by owner."""
        user_id = self._get_user_id(owner)
        sql = """
            SELECT name, source_type, created_at, updated_at
              FROM QBL_CATALOG_REGISTRY
             WHERE (:user_id IS NULL AND owner_user_id IS NULL
                    OR owner_user_id = :user_id)
             ORDER BY updated_at DESC
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, user_id=user_id)
            rows = cur.fetchall()
        return [
            {
                "name":        r[0],
                "source_type": r[1],
                "created_at":  r[2].isoformat() if r[2] else None,
                "updated_at":  r[3].isoformat() if r[3] else None,
            }
            for r in rows
        ]

    def get(self, owner: str, name: str) -> Catalog | None:
        """Retrieve a saved Catalog by owner + name. Returns None if not found."""
        user_id = self._get_user_id(owner)
        sql = """
            SELECT catalog_id, name, alias, label, namespace,
                   catalog_version, description, source_type, catalog_limit, catalog_offset,
                   entities, filters, joins, sort_columns, assignments, properties
              FROM QBL_CATALOG_REGISTRY
             WHERE name = :name
               AND (:user_id IS NULL AND owner_user_id IS NULL
                    OR owner_user_id = :user_id)
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, name=name, user_id=user_id)
            row = cur.fetchone()
        if row is None:
            return None

        (catalog_id, cat_name, alias, label, namespace,
         cat_version, description, source_type, cat_limit, cat_offset,
         entities, filters, joins, sort_columns, assignments, properties) = row

        def _read_clob(val: Any) -> Any:
            if val is None:
                return None
            raw = val.read() if hasattr(val, "read") else val
            return json.loads(raw) if raw else None

        share_scope: str = "SYSTEM" if user_id is None else "USER"
        return Catalog.model_validate({
            "catalog_id":      catalog_id,
            "name":            cat_name,
            "alias":           alias,
            "label":           label,
            "namespace":       namespace,
            "catalog_version": int(cat_version) if cat_version else 1,
            "description":     description,
            "share_scope":     share_scope,
            "owner_user_id":   None if owner == "SYSTEM" else owner,
            "source_type":     source_type,
            "catalog_limit":   int(cat_limit)  if cat_limit  else None,
            "catalog_offset":  int(cat_offset) if cat_offset else None,
            "entities":        _read_clob(entities)     or [],
            "filters":         _read_clob(filters)      or [],
            "joins":           _read_clob(joins)         or [],
            "sort_columns":    _read_clob(sort_columns)  or [],
            "assignments":     _read_clob(assignments)  or [],
            "properties":      _read_clob(properties)   or {},
        })

    def delete(self, owner: str, name: str) -> bool:
        """Delete a saved catalog by name. Returns True if found."""
        user_id = self._get_user_id(owner)
        sql = """
            DELETE FROM QBL_CATALOG_REGISTRY
             WHERE name = :name
               AND (:user_id IS NULL AND owner_user_id IS NULL
                    OR owner_user_id = :user_id)
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, name=name, user_id=user_id)
            deleted = cur.rowcount > 0
        con.commit()
        logger.info("CatalogRegistry: %s '%s' for owner '%s'",
                    "deleted" if deleted else "not found", name, owner)
        return deleted
