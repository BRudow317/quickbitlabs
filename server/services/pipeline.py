from __future__ import annotations

from server.connectors.sf.client import SalesforceClient
from server.connectors.sf.source import SalesforceSource
from server.connectors.supabase.destination import PostgresDestination

def run_sync():
    # 1. Initialize the Source (Salesforce)
    sf_client = SalesforceClient(
        consumer_key="YOUR_KEY", 
        consumer_secret="YOUR_SECRET", 
        instance_url="YOUR_URL"
    )
    source = SalesforceSource(sf_client)

    # 2. Initialize the Destination (Supabase)
    db_url = "postgresql://postgres:your_password@db.your-project.supabase.co:5432/postgres"
    destination = PostgresDestination(db_url)

    # 3. Define the pipeline targets
    streams_to_sync = ["Contact", "Account"]

    # 4. Execute the Agnostic Engine
    for stream in streams_to_sync:
        print(f"\n--- Syncing {stream} ---")
        
        # A. Discover Schema (Returns UniversalTable)
        table_schema = source.discover_schema(streams=[stream])[0]
        
        # B. Apply Schema (Translates UniversalTable -> Postgres DDL)
        destination.apply_schema(table_schema)
        
        # C. Read Data (Returns Generator of Dicts via Bulk V2)
        records_iterator = source.read_data(stream)
        
        # D. Write Data (Consumes Generator, executes Postgres Upserts)
        destination.write_data(stream, records_iterator)

if __name__ == "__main__":
    run_sync()