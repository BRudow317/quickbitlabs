from __future__ import annotations

import pyarrow as pa

from server.plugins.PluginModels import Column, Locator, arrow_type_literal


def pa_type_to_literal(t: pa.DataType) -> arrow_type_literal:
    """Map a PyArrow DataType to its arrow_type_literal string."""
    if pa.types.is_null(t):         return "null"
    if pa.types.is_boolean(t):      return "bool"
    if pa.types.is_int8(t):         return "int8"
    if pa.types.is_int16(t):        return "int16"
    if pa.types.is_int32(t):        return "int32"
    if pa.types.is_int64(t):        return "int64"
    if pa.types.is_uint8(t):        return "uint8"
    if pa.types.is_uint16(t):       return "uint16"
    if pa.types.is_uint32(t):       return "uint32"
    if pa.types.is_uint64(t):       return "uint64"
    if pa.types.is_float16(t):      return "float16"
    if pa.types.is_float32(t):      return "float32"
    if pa.types.is_float64(t):      return "float64"
    if pa.types.is_decimal(t):      return "decimal128"
    if pa.types.is_large_string(t): return "large_string"
    if pa.types.is_string(t):       return "utf8"
    if pa.types.is_large_binary(t): return "large_binary"
    if pa.types.is_binary(t):       return "binary"
    if pa.types.is_date32(t):       return "date32"
    if pa.types.is_date64(t):       return "date64"
    if pa.types.is_timestamp(t):    return f"timestamp_{t.unit}"  # type: ignore[attr-defined]
    if pa.types.is_time32(t):       return f"time32_{t.unit}"     # type: ignore[attr-defined]
    if pa.types.is_time64(t):       return f"time64_{t.unit}"     # type: ignore[attr-defined]
    if pa.types.is_duration(t):     return f"duration_{t.unit}"   # type: ignore[attr-defined]
    if pa.types.is_list(t):         return "list"
    if pa.types.is_large_list(t):   return "large_list"
    if pa.types.is_struct(t):       return "struct"
    if pa.types.is_map(t):          return "map"
    if pa.types.is_dictionary(t):   return "dictionary"
    return "string"


def schema_to_columns(schema: pa.Schema, locator: Locator) -> list[Column]:
    """Convert a PyArrow schema to Column objects with the given locator on each."""
    columns: list[Column] = []
    for field in schema:
        t = field.type
        literal = pa_type_to_literal(t)
        kwargs: dict = {
            "name": field.name,
            "raw_type": str(t),
            "arrow_type_id": literal,
            "is_nullable": field.nullable,
            "locator": locator,
        }
        if pa.types.is_decimal(t):
            kwargs["precision"] = t.precision  # type: ignore[attr-defined]
            kwargs["scale"] = t.scale          # type: ignore[attr-defined]
        elif pa.types.is_timestamp(t) and t.tz:  # type: ignore[attr-defined]
            kwargs["timezone"] = t.tz          # type: ignore[attr-defined]
        columns.append(Column(**kwargs))
    return columns
