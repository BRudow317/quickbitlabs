"""
Live Salesforce -> Oracle Migration Test.
Verifies full discovery hydration and end-to-end data transfer.

python Q:/scripts/boot.py -v -l ./.logs --env homelab --config Q:/.secrets/.env --exec pytest server/services/tests/test_migration_live.py
"""
import pytest
import pyarrow as pa
from server.services.CatalogMigration import CatalogMigration
from server.plugins.PluginModels import Catalog, Entity, Column
from server.plugins.PluginRegistry import get_plugin

def test_sf_to_oracle_live_migration():
    # 1. Setup - Targeted migration of just 'Account'
    # We provide the entity name; SfService.get_catalog should hydrate the columns.
    source_catalog = Catalog(entities=[Entity(name="Account")])
    target_catalog = Catalog() # Use default ORACLE_USER schema
    
    job = CatalogMigration(
        source_plugin="salesforce",
        target_plugin="oracle",
        source_catalog=source_catalog,
        target_catalog=target_catalog
    )
    
    # 2. Discovery - Should now hydrate columns for Account
    print("\nStarting Discovery...")
    job.get_catalog()
    
    assert len(job.source_catalog.entities) == 1
    account = job.source_catalog.entities[0]
    print(f"Discovered entity: {account.name} with {len(account.columns)} columns.")
    assert account.name == "Account"
    assert len(account.columns) > 0, "Account entity should be hydrated with columns"
    
    # 3. Mapping - Change target name to TEST_ACCOUNT for idempotency and safety
    print("Mapping to TEST_ACCOUNT...")
    test_entity = account.model_copy(deep=True)
    test_entity.name = "TEST_ACCOUNT"
    
    # Update locators to point to the new test table in Oracle
    pk_col = None
    for col in test_entity.columns:
        if col.locator:
            col.locator.entity_name = "TEST_ACCOUNT"
            col.locator.plugin = "oracle"
        if col.primary_key:
            pk_col = col
    
    job.target_catalog.entities = [test_entity]
    
    # Add MERGE ON clause (Framework requirement for Oracle upsert)
    if pk_col:
        from server.plugins.PluginModels import OperatorGroup, Operation
        job.target_catalog.operator_groups = [
            OperatorGroup(
                condition="AND",
                operation_group=[
                    Operation(
                        independent=pk_col,
                        operator="==",
                        dependent=pa.field(pk_col.name)
                    )
                ]
            )
        ]
    
    # 4. DDL - Create/Align the TEST_ACCOUNT table
    print("Executing Target DDL...")
    resp = job.target.upsert_catalog(job.target_catalog)
    assert resp.ok, f"Oracle DDL failed: {resp.message}"
    
    # 5. Data Transfer - Migrate a small sample
    print("Starting Data Transfer...")
    job.source_catalog.limit = 5
    # The upsert_data loop uses job.target_catalog.entities
    results = job.upsert_data()
    
    assert results[0]["status"] == "ok", f"Migration failed: {results[0].get('message')}"
    
    # 6. Verification - Read back from Oracle to confirm data exists
    print("Verifying data in Oracle...")
    verify_resp = job.target.get_data(job.target_catalog)
    assert verify_resp.ok
    
    table = verify_resp.data.read_all()
    print(f"Migrated {table.num_rows} rows to TEST_ACCOUNT.")
    assert table.num_rows > 0
    
    # 7. Cleanup - Drop the test table to keep the environment clean
    print("Cleaning up...")
    cleanup_resp = job.target.delete_entity(job.target_catalog)
    assert cleanup_resp.ok, f"Failed to cleanup TEST_ACCOUNT table: {cleanup_resp.message}"
    print("Test Complete.")
