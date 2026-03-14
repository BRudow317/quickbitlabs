from __future__ import annotations

import csv
import io
from typing import Any
from collections.abc import Iterator
from core.interfaces import SourceConnector
from server.models.schema import UniversalTable, UniversalColumn
from client import SalesforceClient

class SalesforceSource(SourceConnector):
    """
    Extracts data and schema from Salesforce, translating it into 
    the engine's agnostic Intermediate Representation (IR).
    """

    def __init__(self, client: SalesforceClient):
        self.sf = client
        
        # The Rosetta Stone: Salesforce Types -> Universal Types
        self.type_map = {
            'string': 'string',
            'id': 'string',
            'reference': 'string',
            'textarea': 'string',
            'picklist': 'string',
            'multipicklist': 'string',
            'phone': 'string',
            'email': 'string',
            'url': 'string',
            'boolean': 'boolean',
            'int': 'integer',
            'double': 'float',
            'currency': 'float',
            'percent': 'float',
            'date': 'date',
            'datetime': 'datetime'
        }

    def discover_schema(self, streams: list[str] | None = None) -> list[UniversalTable]:
        """
        Hits the Salesforce Describe API and translates the proprietary JSON
        into our agnostic UniversalTable Pydantic models.
        """
        if not streams:
            # If no streams provided, you could theoretically hit the Global Describe
            # and return everything, but for safety, we'll require explicit streams.
            raise ValueError("Must provide a list of Salesforce objects to discover.")

        universal_tables = []

        for object_name in streams:
            # 1. Fetch the massive schema payload via our REST service
            sf_obj = getattr(self.sf.rest, object_name)
            raw_describe = sf_obj.describe()
            
            columns = []
            
            # 2. Iterate through the Salesforce fields and translate them
            for field in raw_describe.get('fields', []):
                # We only want fields we can actually pull data from
                if not field.get('queryByDistance') and field.get('type') != 'address': 
                    
                    # Fallback to 'string' if Salesforce adds a crazy new type
                    universal_type = self.type_map.get(field['type'], 'string')
                    
                    col = UniversalColumn(
                        name=field['name'],
                        datatype=universal_type,
                        primary_key=(field['type'] == 'id'),
                        nullable=field['nillable'],
                        length=field.get('length')
                    )
                    columns.append(col)

            # 3. Create the finalized agnostic table definition
            table = UniversalTable(
                name=raw_describe['name'],
                columns=columns
            )
            universal_tables.append(table)

        return universal_tables

    def read_data(self, stream_name: str) -> Iterator[dict[str, Any]]:
        """
        Dynamically builds a SOQL query based on the schema, executes it 
        via the Bulk 2.0 API, and yields rows as agnostic dictionaries.
        """
        # 1. Discover the schema to get the columns to query
        table_schema = self.discover_schema(streams=[stream_name])[0]
        
        # 2. Build the dynamic SOQL Query
        # "SELECT Id, FirstName, LastName FROM Contact"
        field_names = [col.name for col in table_schema.columns]
        soql_query = f"SELECT {', '.join(field_names)} FROM {stream_name}"
        
        print(f"Executing dynamic bulk query: {soql_query[:100]}...")

        # 3. Execute the Bulk Query via our Bulk2 service
        bulk_obj = getattr(self.sf.bulk2, stream_name)
        
        # The bulk2 query yields raw CSV string chunks. 
        # We need to parse them into dictionaries to satisfy our interface.
        for csv_chunk in bulk_obj.query(soql_query):
            # Use StringIO to make the string behave like a file for the csv module
            chunk_stream = io.StringIO(csv_chunk)
            reader = csv.DictReader(chunk_stream)
            
            for row in reader:
                yield row