"""
CatalogPublisher service: AST -> Oracle Metadata Registry.

python Q:/scripts/boot.py -v -l ./.logs --env homelab --config Q:/.secrets/.env --exec ./publish.py
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Literal

import oracledb

from server.plugins.PluginModels import Catalog

logger = logging.getLogger(__name__)

class CatalogPublisher:
    """
    Orchestrates the registration and update of a Catalog AST into the 
    Oracle Unified Registry database.
    
    Input:
    ---
        - catalog: The Pydantic Catalog object to register.
        - name: The canonical name of the catalog (e.g., 'Q3 Risk Summary').
        - namespace: The grouping folder (e.g., 'salesforce', 'actuary_marts').
        - scope: Authorization level ('SYSTEM', 'TEAM', 'USER').
        - is_materialized: Boolean flag indicating if this relies on a physical Oracle view.
        - oracle_view_name: The name of the Oracle MV (Required if is_materialized=True).
        - owner_user_id: The UUID of the user who owns this (if USER scope).
        - owner_team_id: The UUID of the team who owns this (if TEAM scope).
        - db_kwargs: Dictionary containing Oracle connection credentials.
    """
    catalog: Catalog
    name: str
    namespace: str
    scope: Literal["SYSTEM", "TEAM", "USER"]
    is_materialized: int
    view_name: str | None
    owner_user_id: str | None
    owner_team_id: str | None
    db_kwargs: dict[str, Any]

    def __init__(
        self,
        catalog: Catalog,
        name: str,
        namespace: str,
        scope: Literal["SYSTEM", "TEAM", "USER"] = "SYSTEM",
        is_materialized: bool = False,
        view_name: str | None = None,
        owner_user_id: str | None = None,
        owner_team_id: str | None = None,
        db_kwargs: dict[str, Any] | None = None
    ):
        self.catalog = catalog
        self.name = name
        self.namespace = namespace
        self.scope = scope
        self.is_materialized = 1 if is_materialized else 0
        self.view_name = view_name
        self.owner_user_id = owner_user_id
        self.owner_team_id = owner_team_id
        self.db_kwargs = db_kwargs or {}

    # ------------------------------------------------------------------
    # Step 1: Validation
    # ------------------------------------------------------------------
    def validate(self) -> None:
        """Ensure the metadata payload conforms to database constraints."""
        if self.is_materialized and not self.view_name:
            raise ValueError("view_name must be provided if is_materialized is True.")
            
        if self.scope == "USER" and not self.owner_user_id:
            raise ValueError("owner_user_id is required for USER scope catalogs.")
            
        if self.scope == "TEAM" and not self.owner_team_id:
            raise ValueError("owner_team_id is required for TEAM scope catalogs.")
            
        # Ensure the catalog can cleanly dump to JSON without null bloat
        try:
            self._json_payload = self.catalog.model_dump_json(exclude_none=True)
        except Exception as e:
            raise RuntimeError(f"Failed to serialize Catalog to JSON: {e}")

    # ------------------------------------------------------------------
    # Step 2: Database Connection
    # ------------------------------------------------------------------
    def _get_connection(self) -> oracledb.Connection:
        """Instantiate the Oracle connection using provided kwargs."""
        return oracledb.connect(
            user=self.db_kwargs.get("user"),
            password=self.db_kwargs.get("password"),
            dsn=self.db_kwargs.get("dsn")
        )

    # ------------------------------------------------------------------
    # Step 3: Registry Upsert
    # ------------------------------------------------------------------
    def upsert_registry(self) -> str:
        """MERGE the catalog JSON into the Oracle registry."""
        logger.info(f"Step 2: Upserting '{self.namespace}.{self.name}' to Oracle Registry...")
        
        sql = """
        MERGE INTO catalog_registry trg
        USING (
            SELECT :catalog_id AS catalog_id,
                   :name AS name,
                   :namespace AS namespace,
                   :scope AS scope,
                   :owner_user_id AS owner_user_id,
                   :owner_team_id AS owner_team_id,
                   :catalog_json AS catalog_json,
                   :is_materialized AS is_materialized,
                   :view_name AS view_name
            FROM dual
        ) src
        ON (trg.name = src.name AND trg.namespace = src.namespace)
        WHEN MATCHED THEN
            UPDATE SET 
                scope = src.scope,
                owner_user_id = src.owner_user_id,
                owner_team_id = src.owner_team_id,
                catalog_json = src.catalog_json,
                is_materialized = src.is_materialized,
                view_name = src.view_name,
                updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN
            INSERT (catalog_id, name, namespace, scope, owner_user_id, owner_team_id, catalog_json, is_materialized, view_name)
            VALUES (src.catalog_id, src.name, src.namespace, src.scope, src.owner_user_id, src.owner_team_id, src.catalog_json, src.is_materialized, src.view_name)
        """
        
        # Pre-generate a UUID in case this is a new insert
        catalog_id = str(uuid.uuid4())
        
        binds = {
            "catalog_id": catalog_id,
            "name": self.name,
            "namespace": self.namespace,
            "scope": self.scope,
            "owner_user_id": self.owner_user_id,
            "owner_team_id": self.owner_team_id,
            "catalog_json": self._json_payload,
            "is_materialized": self.is_materialized,
            "view_name": self.view_name
        }

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # oracledb handles mapping the Python string to the Oracle CLOB natively
                    cursor.execute(sql, binds)
                    conn.commit()
                    
            logger.info(f"    Success: Catalog registered securely.")
            return catalog_id
            
        except oracledb.DatabaseError as e:
            error_obj, = e.args
            logger.error(f"Oracle Database Error [{error_obj.code}]: {error_obj.message}")
            raise RuntimeError(f"Registry Upsert Failed: {error_obj.message}")

def dummy_job() -> None:
    """Entry point for the bootloader."""
    # Example: Registering a team's materialized view
    dummy_catalog = Catalog(name="Risk_View", entities=[])
    
    publisher = CatalogPublisher(
        catalog=dummy_catalog,
        name="Q3 Actuary Risk Summary",
        namespace="actuary_marts",
        scope="TEAM",
        owner_team_id="team-2001",
        is_materialized=True,
        view_name="MV_Q3_RISK_SUMMARY",
        db_kwargs={"user": "admin", "password": "pwd", "dsn": "localhost:1521/XEPDB1"}
    )
    
    publisher.validate()
    publisher.upsert_registry()

if __name__ == "__main__":
    dummy_job()