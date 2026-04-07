import pyarrow as pa
import json

# Your base schema generated from the Catalog
base_schema = pa.schema([
    pa.field("salesforce.id", pa.string()),
    pa.field("oracle.balance", pa.decimal128(38, 9))
])

# 1. Serialize your query context to bytes
query_context = {
    "source_systems": ["salesforce", "oracle"],
    "generated_by": "federated_engine_v1",
    "is_aggregated": True
}
encoded_meta = {b"federated_context": json.dumps(query_context).encode('utf-8')}

# 2. Attach it to the schema
final_schema = base_schema.with_metadata(encoded_meta)

# Now, downstream systems can read final_schema.metadata to know 
# exactly where this stream came from!