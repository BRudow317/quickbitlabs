# quickbitlabs/server/engine/sync.py

from server.core.interfaces import SourceConnector, DestinationConnector

def run_sync(source: SourceConnector, destination: DestinationConnector, streams: list[str]) -> None:
    """
    A completely agnostic sync engine. It doesn't know what Salesforce 
    or Postgres are. It only knows about the Interface contracts.
    """
    
    # 1. Discover the schemas
    universal_tables = source.discover_schema(streams=streams)

    for table in universal_tables:
        # 2. Tell the destination to prepare its database tables
        destination.apply_schema(table)
        
        # 3. Stream the data out of the source
        record_iterator = source.read_data(stream_name=table.name)
        
        # 4. Stream the data into the destination
        destination.write_data(stream_name=table.name, records=record_iterator)

    print("Sync complete!")