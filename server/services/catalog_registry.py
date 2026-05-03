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
    Persists Catalog snapshots in CATALOG_REGISTRY.

    Scalar Catalog fields are columns (fast filtering/listing without JSON parsing).
    List fields and properties are individual JSON CLOB columns on the same row.

    catalog.name is required and is the unique discriminator within an owner's scope.
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

    def _scope_for(self, owner: str) -> str:
        return "SYSTEM" if owner == "SYSTEM" else "USER"

    def _get_registry_id(self, scope: str, owner_user_id: int | None, name: str) -> int | None:
        sql = """
            SELECT catalog_registry_id FROM CATALOG_REGISTRY
             WHERE scope          = :scope
               AND (owner_user_id = :user_id OR (:user_id IS NULL AND owner_user_id IS NULL))
               AND name           = :name
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, scope=scope, user_id=owner_user_id, name=name)
            row = cur.fetchone()
        return int(row[0]) if row else None

    def _dump_json_fields(self, catalog: Catalog) -> dict[str, str | None]:
        data = catalog.model_dump(mode="json")
        result: dict[str, str | None] = {}
        for field in _JSON_FIELDS:
            val = data.get(field)
            result[field] = json.dumps(val) if val else None
        return result

    # ================================================
    # CRUD
    # ================================================

    def save(self, owner: str, catalog: Catalog) -> None:
        """Upsert a catalog for owner using catalog.name as the unique key."""
        if not catalog.name:
            raise ValueError("catalog.name is required to save to the registry")

        scope   = self._scope_for(owner)
        user_id = self._get_user_id(owner)
        con     = self._server_db.connect()

        registry_id = self._get_registry_id(scope, user_id, catalog.name)
        jf = self._dump_json_fields(catalog)

        _clob_fields = ("entities", "filters", "joins", "sort_columns", "assignments", "properties")

        if registry_id is None:
            sql = """
                INSERT INTO CATALOG_REGISTRY
                    (scope, owner_user_id,
                     catalog_id, name, alias, namespace, catalog_version,
                     description, source_type, catalog_limit, catalog_offset,
                     entities, filters, joins, sort_columns, assignments, properties,
                     created_at, created_by, updated_at, updated_by)
                VALUES
                    (:scope, :user_id,
                     :catalog_id, :name, :alias, :namespace, :cat_version,
                     :description, :source_type, :cat_limit, :cat_offset,
                     :entities, :filters, :joins, :sort_columns, :assignments, :properties,
                     SYSTIMESTAMP, :owner, SYSTIMESTAMP, :owner)
                RETURNING catalog_registry_id INTO :reg_id
            """
            with con.cursor() as cur:
                cur.setinputsizes(**{f: DB_TYPE_CLOB for f in _clob_fields})
                reg_id_var = cur.var(oracledb.NUMBER)
                cur.execute(sql, {
                    "scope": scope, "user_id": user_id,
                    "catalog_id": catalog.catalog_id,
                    "name": catalog.name, "alias": catalog.alias,
                    "namespace": catalog.namespace, "cat_version": catalog.version,
                    "description": catalog.description, "source_type": catalog.source_type,
                    "cat_limit": catalog.limit, "cat_offset": catalog.offset,
                    "owner": owner, "reg_id": reg_id_var,
                    **jf,
                })
                con.commit()
            raw = reg_id_var.getvalue()
            registry_id = int(raw[0]) if raw else None
        else:
            sql = """
                UPDATE CATALOG_REGISTRY
                   SET catalog_id      = :catalog_id,
                       alias           = :alias,
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
                 WHERE catalog_registry_id = :reg_id
            """
            with con.cursor() as cur:
                cur.setinputsizes(**{f: DB_TYPE_CLOB for f in _clob_fields})
                cur.execute(sql, {
                    "catalog_id": catalog.catalog_id,
                    "alias": catalog.alias,
                    "namespace": catalog.namespace, "cat_version": catalog.version,
                    "description": catalog.description, "source_type": catalog.source_type,
                    "cat_limit": catalog.limit, "cat_offset": catalog.offset,
                    "owner": owner, "reg_id": registry_id,
                    **jf,
                })
                con.commit()

        if registry_id is None:
            raise RuntimeError(f"Failed to upsert CATALOG_REGISTRY for owner={owner!r} name={catalog.name!r}")

        logger.info("CatalogRegistry: saved '%s' for owner '%s'", catalog.name, owner)

    def list_entries(self, owner: str) -> list[dict[str, Any]]:
        """Return lightweight metadata for all catalogs saved by owner (no JSON parsing)."""
        scope   = self._scope_for(owner)
        user_id = self._get_user_id(owner)
        sql = """
            SELECT name, source_type, created_at, updated_at
              FROM CATALOG_REGISTRY
             WHERE scope = :scope
               AND (owner_user_id = :user_id OR (:user_id IS NULL AND owner_user_id IS NULL))
             ORDER BY updated_at DESC
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, scope=scope, user_id=user_id)
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
        scope   = self._scope_for(owner)
        user_id = self._get_user_id(owner)
        sql = """
            SELECT catalog_id, name, alias, namespace,
                   catalog_version, description, source_type, catalog_limit, catalog_offset,
                   entities, filters, joins, sort_columns, assignments, properties
              FROM CATALOG_REGISTRY
             WHERE scope          = :scope
               AND (owner_user_id = :user_id OR (:user_id IS NULL AND owner_user_id IS NULL))
               AND name           = :name
        """
        with self._server_db.connect().cursor() as cur:
            cur.execute(sql, scope=scope, user_id=user_id, name=name)
            row = cur.fetchone()
        if row is None:
            return None

        (catalog_id, cat_name, alias, namespace,
         cat_version, description, source_type, cat_limit, cat_offset,
         entities, filters, joins, sort_columns, assignments, properties) = row

        def _read_clob(val: Any) -> Any:
            if val is None:
                return None
            raw = val.read() if hasattr(val, "read") else val
            return json.loads(raw) if raw else None

        return Catalog.model_validate({
            "catalog_id":  catalog_id,
            "name":        cat_name,
            "alias":       alias,
            "namespace":   namespace,
            "version":     int(cat_version) if cat_version else 1,
            "description": description,
            "scope":       scope,
            "source_type": source_type,
            "limit":       int(cat_limit)  if cat_limit  else None,
            "offset":      int(cat_offset) if cat_offset else None,
            "entities":    _read_clob(entities)    or [],
            "filters":     _read_clob(filters)     or [],
            "joins":       _read_clob(joins)        or [],
            "sort_columns": _read_clob(sort_columns) or [],
            "assignments": _read_clob(assignments) or [],
            "properties":  _read_clob(properties)  or {},
        })

    def delete(self, owner: str, name: str) -> bool:
        """Delete a saved catalog by name. Returns True if found."""
        scope   = self._scope_for(owner)
        user_id = self._get_user_id(owner)
        sql = """
            DELETE FROM CATALOG_REGISTRY
             WHERE scope          = :scope
               AND (owner_user_id = :user_id OR (:user_id IS NULL AND owner_user_id IS NULL))
               AND name           = :name
        """
        con = self._server_db.connect()
        with con.cursor() as cur:
            cur.execute(sql, scope=scope, user_id=user_id, name=name)
            deleted = cur.rowcount > 0
        con.commit()
        logger.info("CatalogRegistry: %s '%s' for owner '%s'",
                    "deleted" if deleted else "not found", name, owner)
        return deleted
