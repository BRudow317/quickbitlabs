from __future__ import annotations

import pyarrow as pa

from server.plugins.PluginModels import Column, Locator, pa_type_to_literal, pa_type_to_meta


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

        meta = pa_type_to_meta(t)
        if meta is not None:
            kwargs["arrow_type_meta"] = meta

        columns.append(Column(**kwargs))
    return columns
