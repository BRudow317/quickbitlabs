from __future__ import annotations
import pyarrow as pa
from typing import Iterator
from server.plugins.PluginModels import ArrowReader

def rename_stream(arrow_reader: ArrowReader, name_map: dict[str, str]) -> ArrowReader:
    """Rename columns in an ArrowReader according to a name mapping.
    Keys not in the map are kept as-is."""
    schema = arrow_reader.schema
    if not schema:
        return arrow_reader
    new_names = [name_map.get(f.name, f.name) for f in schema]
    new_fields = [
        pa.field(new_names[i], schema.field(i).type, schema.field(i).nullable)
        for i in range(len(schema))
    ]
    new_schema = pa.schema(new_fields)

    def _batches() -> Iterator[pa.RecordBatch]:
        for batch in arrow_reader:
            yield batch.rename_columns(new_names)

    return pa.RecordBatchReader.from_batches(new_schema, _batches())