import pyarrow as pa
from typing import Iterator, Iterable, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from server.plugins.PluginModels import Catalog, Records

"""
1. Iterator[pa.RecordBatch] (The Python Concept)
This is not an Arrow concept; it is a standard Python type hint.
It just means: "I have a Python function (usually a generator with yield) that is going to hand you pa.RecordBatch objects one at a time in a for loop."

The Problem: Pure C++ or Rust engines (like Polars) cannot natively speak to a Python Iterator. Furthermore, an iterator doesn't tell you what the schema is until you pull the very first batch out of it.

Where you use it: You use this inside your plugins to pull data out of your databases or APIs chunk by chunk.

2. pa.RecordBatchReader (The C++ Pipe)
This is the actual C++ interface (specifically, the Arrow C Stream Interface).
It is a strictly defined, standardized "pipe" that holds two things:

A rigid pa.Schema (the blueprint).

A stream of pa.RecordBatch objects that perfectly match that schema.

The Superpower: Because this is a standardized C++ object, when you hand it to Polars, Polars bypasses the Python interpreter entirely and plugs its Rust engine directly into the C++ pipe. This is where zero-copy streaming happens.

Where you use it: This is your Universal Contract. It is the return type of every plugin, and the input type for your orchestrator.

3. pa.RecordBatchReader.from_batches() (The Bridge)
This is a constructor (a factory method). You use this to convert #1 into #2.

Because your plugins are written in Python, you are going to be generating Python iterators. But because your contract demands a C++ pipe, you need a way to bridge the gap. from_batches(schema, iterator) takes your pure Python generator, attaches the explicit schema to the front of it, and wraps it in the C++ RecordBatchReader interface.
"""
import pyarrow as pa
from typing import Iterator, Iterable, Any

case_schema = pa.schema([
    pa.field("case_id", pa.string()),
    pa.field("case_subject", pa.string()),
    pa.field("case_description", pa.string()),
    pa.field("create_date", pa.timestamp('s'))
])

def rest_streaming_batcher(
    json_row_stream: Iterable[dict[str, Any]], 
    schema: pa.Schema,
    chunk_size: int = 50_000
) -> pa.RecordBatchReader:
    
    def batch_generator() -> Iterator[pa.RecordBatch]:
        """Internal generator that chunks the endless stream."""
        ids, subjects, descriptions, dates = [], [], [], []
        row_count = 0
        
        for row in json_row_stream:
            ids.append(row.get("case_id"))
            subjects.append(row.get("case_subject"))
            descriptions.append(row.get("case_description"))
            dates.append(row.get("create_date"))
            
            row_count += 1
            
            # When we hit the limit, yield the batch and CLEAR the memory
            if row_count == chunk_size:
                yield pa.record_batch([ids, subjects, descriptions, dates], schema=schema)
                
                # Reset the lists to free up RAM for the next chunk
                ids, subjects, descriptions, dates = [], [], [], []
                row_count = 0
                
        # Don't forget to yield the final, partially-filled chunk!
        if row_count > 0:
            yield pa.record_batch([ids, subjects, descriptions, dates], schema=schema)

    # Wrap the generator in the C++ pipe
    return pa.RecordBatchReader.from_batches(schema, batch_generator())



def stream_to_json(reader: pa.RecordBatchReader) -> Iterator[list[dict[str, Any]]]:
    
    # The Reader acts as an iterator. Every time you loop, 
    # it hands you the next pa.RecordBatch.
    for batch in reader:
        
        # THE MAGIC TRICK:
        # to_pylist() takes the columnar C++ batch and instantly pivots it 
        # into a standard Python list of dictionaries.
        # e.g., [{"case_id": "1", "case_subject": "Broken Server"}, ...]
        
        json_chunk = batch.to_pylist()
        
        yield json_chunk

# --- How you use it in your API / Egress Layer ---

def my_fastapi_endpoint():
    # 1. Get the stream from your orchestrator/Polars engine
    my_reader = get_json_input() 
    
    # 2. Consume it safely
    for json_chunk in stream_to_json(my_reader):
        # json_chunk is just a normal Python list of dicts.
        # You can now hand this to FastAPI, send it to a webhook,
        # or push it back into a legacy system that only speaks JSON.
        
        # e.g., send_to_legacy_system(json_chunk)
        pass


# Safe json encoder from arrow streams
import json
from decimal import Decimal
from datetime import datetime

def safe_json_encoder(obj):
    if isinstance(obj, Decimal):
        return float(obj) # Or str(obj) if you want to avoid JS float rounding
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

# Usage:
# json.dumps(json_chunk, default=safe_json_encoder)
"""
1. Core Properties (Inspection)
These are the quickest ways to grab the shape of the schema without iterating through it.

schema.names: Returns a list of strings representing the column names in order.

schema.types: Returns a list of pa.DataType objects in order.

schema.empty: Returns a boolean (True if the schema has zero fields).

2. Accessing Fields (pa.Field objects)
A PyArrow Schema is essentially an ordered collection of pa.Field objects. (A pa.Field contains the name, type, and nullability).

schema.field(i): The most common accessor. You can pass an integer (index) or a string (column name) to get the specific pa.Field object.

schema.get_field_index(name): Returns the integer index of a column by its string name. (Throws a ValueError if it doesn't exist).

schema.get_all_field_indices(name): Arrow technically allows duplicate column names. This returns a list of indices for all fields matching that name.

3. Schema Modification
Because Arrow schemas are immutable at the C++ level, these methods don't change the schema in place; they return a brand new pa.Schema with the requested changes.

schema.append(field): Returns a new schema with a pa.Field added to the end.

schema.insert(index, field): Returns a new schema with a pa.Field inserted at the specified integer index.

schema.remove(index): Returns a new schema with the field at that index removed.

schema.set(index, field): Returns a new schema where the field at the specified index is replaced by a new pa.Field.

4. Metadata (The "Hidden" Superpower)
This is highly relevant for your Catalog architecture. You can attach arbitrary key-value byte strings to an Arrow schema. This metadata survives Parquet serialization and IPC streaming, meaning you can pass federated query context right alongside the data.

schema.metadata: Returns a dictionary of the attached metadata (keys and values are usually bytes). If none exists, returns None.

schema.with_metadata(metadata_dict): Returns a new schema with the provided dictionary attached as metadata.

schema.serialize(): Serializes the schema to an Arrow IPC buffer (useful if you need to send just the schema blueprint over a network before the data arrives).
"""