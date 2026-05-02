from typing import Any, Literal
import oracledb
from server.plugins.PluginModels import Column, ARROW_TYPE, arrow_type_literal, pa_type_to_literal
import pyarrow as pa

def map_oracle_to_arrow(raw_type: str, scale: int | None = None) -> arrow_type_literal:
    raw_upper = raw_type.upper()
    if raw_upper in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "ROWID", "UROWID"): return "string"
    if raw_upper in ("CLOB", "NCLOB", "JSON"): return "large_string"
    if raw_upper == "NUMBER": return "int64" if scale == 0 else "float64"
    if raw_upper in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"): return "float64"
    if "TIMESTAMP" in raw_upper: return "timestamp_ns"
    if raw_upper == "DATE": return "timestamp_s"
    if raw_upper in ("RAW", "LONG RAW"): return "binary"
    if raw_upper in ("BLOB", "BFILE"): return "large_binary"
    return "string"

_INT_ORDER:   list[arrow_type_literal] = ["int8",   "int16",  "int32",  "int64"]
_UINT_ORDER:  list[arrow_type_literal] = ["uint8",  "uint16", "uint32", "uint64"]
_FLOAT_ORDER: list[arrow_type_literal] = ["float16","float32","float64"]
_TEMPORAL_ORDER: list[arrow_type_literal] = [
    "date32","date64","timestamp_s","timestamp_ms","timestamp_us","timestamp_ns"
]
_INT_SET   = frozenset(_INT_ORDER)
_UINT_SET  = frozenset(_UINT_ORDER)
_FLOAT_SET = frozenset(_FLOAT_ORDER)
_DECIMAL_SET  = frozenset({"decimal128", "decimal256"})
_TEMPORAL_SET = frozenset(_TEMPORAL_ORDER)
_BINARY_SET   = frozenset({"binary", "large_binary"})
_STRING_SET   = frozenset({"string", "utf8", "string_view", "large_string", "json", "uuid", "dictionary"})
_NUMERIC_SET  = _INT_SET | _UINT_SET | _FLOAT_SET | _DECIMAL_SET | frozenset({"bool"})


def promote_arrow_types(a: arrow_type_literal, b: arrow_type_literal) -> arrow_type_literal:
    """Return the widest compatible arrow_type_literal that can hold values of both a and b.

    Rules (widest wins, large_string / CLOB is the final fallback for incompatible types):
      - Same type            → identity
      - null + anything      → the other type
      - large_string + any   → large_string  (CLOB is a one-way door)
      - Within int family    → wider int
      - Within uint family   → wider uint
      - Int + uint           → wider signed int (promote to avoid overflow)
      - Int/uint + float     → float64
      - Numeric + decimal    → decimal128
      - bool + numeric       → the numeric type
      - Within float family  → wider float
      - Within temporal      → wider temporal (timestamp_ns is widest)
      - date + timestamp     → timestamp_ns
      - binary + large_bin   → large_binary
      - string variants      → large_string if either is large_string, else string
      - numeric + string     → large_string  (VARCHAR2 can store formatted numbers as last resort)
      - temporal + string    → large_string
      - anything else        → large_string
    """
    if a == b:
        return a
    if a == "null":
        return b
    if b == "null":
        return a
    if "large_string" in (a, b):
        return "large_string"

    # Within string family
    if a in _STRING_SET and b in _STRING_SET:
        for t in ("large_string", "json", "string", "utf8", "string_view", "dictionary", "uuid"):
            if t in (a, b):
                return t  # type: ignore[return-value]

    # Within int family
    if a in _INT_SET and b in _INT_SET:
        return _INT_ORDER[max(_INT_ORDER.index(a), _INT_ORDER.index(b))]

    # Within uint family
    if a in _UINT_SET and b in _UINT_SET:
        return _UINT_ORDER[max(_UINT_ORDER.index(a), _UINT_ORDER.index(b))]

    # Int + uint → promote to next wider signed int to safely cover both ranges
    if (a in _INT_SET and b in _UINT_SET) or (a in _UINT_SET and b in _INT_SET):
        ia = a if a in _INT_SET else b
        ub = b if b in _UINT_SET else a
        idx = min(max(_INT_ORDER.index(ia), _UINT_ORDER.index(ub)) + 1, len(_INT_ORDER) - 1)
        return _INT_ORDER[idx]

    # Within float family
    if a in _FLOAT_SET and b in _FLOAT_SET:
        return _FLOAT_ORDER[max(_FLOAT_ORDER.index(a), _FLOAT_ORDER.index(b))]

    # Int/uint + float → float64
    if (a in _INT_SET | _UINT_SET and b in _FLOAT_SET) or \
       (b in _INT_SET | _UINT_SET and a in _FLOAT_SET):
        return "float64"

    # Decimal + any numeric → decimal128
    if (a in _DECIMAL_SET or b in _DECIMAL_SET) and \
       (a in _NUMERIC_SET and b in _NUMERIC_SET):
        return "decimal128"

    # Bool + numeric → the numeric type
    if "bool" in (a, b):
        other = b if a == "bool" else a
        if other in _NUMERIC_SET | _STRING_SET:
            return other  # type: ignore[return-value]

    # Within temporal family
    if a in _TEMPORAL_SET and b in _TEMPORAL_SET:
        return _TEMPORAL_ORDER[max(_TEMPORAL_ORDER.index(a), _TEMPORAL_ORDER.index(b))]

    # Binary family
    if a in _BINARY_SET and b in _BINARY_SET:
        return "large_binary"

    # Incompatible families → CLOB (safe universal container)
    return "large_string"

ARROW_TO_ORACLE_DDL: dict[arrow_type_literal, str] = {
    "null":             "VARCHAR2(255 CHAR)",
    "bool":             "NUMBER(1, 0)",
    "int8":             "NUMBER",
    "int16":            "NUMBER",
    "int32":            "NUMBER",
    "int64":            "NUMBER",
    "uint8":            "NUMBER",
    "uint16":           "NUMBER",
    "uint32":           "NUMBER",
    "uint64":           "NUMBER",
    "float16":          "NUMBER",
    "float32":          "NUMBER",
    "float64":          "NUMBER",
    "decimal128":       "NUMBER",
    "decimal256":       "NUMBER",
    "string":           "VARCHAR2(255 CHAR)",
    "utf8":             "VARCHAR2(255 CHAR)",
    "string_view":      "VARCHAR2(255 CHAR)",
    "large_string":     "CLOB",
    "binary":           "BLOB",
    "large_binary":     "BLOB",
    "date32":           "DATE",
    "date64":           "DATE",
    "timestamp_s":      "TIMESTAMP",
    "timestamp_ms":     "TIMESTAMP",
    "timestamp_us":     "TIMESTAMP",
    "timestamp_ns":     "TIMESTAMP",
    "time32_s":         "TIMESTAMP",
    "time32_ms":        "TIMESTAMP",
    "time64_us":        "TIMESTAMP",
    "time64_ns":        "TIMESTAMP",
    "duration_s":       "NUMBER",
    "duration_ms":      "NUMBER",
    "duration_us":      "NUMBER",
    "duration_ns":      "NUMBER",
    "list":             "JSON",
    "large_list":       "JSON",
    "list_view":        "JSON",
    "large_list_view":  "JSON",
    "struct":           "JSON",
    "map":              "JSON",
    "dictionary":       "VARCHAR2(4000 CHAR)",
    "json":             "JSON",
    "uuid":             "VARCHAR2(36 CHAR)",
}


def map_arrow_to_oracle_ddl(column: Column) -> str:
    atype = column.arrow_type_id
    if atype is None: raise ValueError(f"Column {column.name} is missing 'arrow_type_id'.")
    if atype in ("string", "utf8", "string_view"):
        # max_length=0 means "unknown" per Column spec, not "short". 4000 CHAR is the safe default.
        length = column.max_length or 4000
        return "CLOB" if length > 4000 else f"VARCHAR2({length} CHAR)"
    if atype == "large_string": return "CLOB"
    if atype.startswith("int") or atype.startswith("uint"): return "NUMBER"
    if atype.startswith("float") or atype.startswith("decimal"):
        return f"NUMBER({column.precision}, {column.scale})" if column.precision and column.scale else "NUMBER"
    if atype == "bool": return "NUMBER(1, 0)"
    if atype.startswith("timestamp"): return "TIMESTAMP WITH TIME ZONE" if column.timezone else "TIMESTAMP"
    if atype.startswith("date"): return "DATE"
    if atype.startswith("time") or atype.startswith("duration"): return "TIMESTAMP"
    if atype in ("binary", "large_binary"): return "BLOB"
    if atype in ("list", "large_list", "list_view", "large_list_view", "struct", "map", "json"):
        return "JSON"
    if atype == "dictionary": return f"VARCHAR2({column.max_length or 4000} CHAR)"
    if atype == "uuid": return "VARCHAR2(36 CHAR)"
    return "VARCHAR2(255 CHAR)"

def map_column_to_oracledb_input_size(column: Column) -> Any:
    atype = column.arrow_type_id
    if atype is None: raise ValueError(f"Column {column.name} is missing 'arrow_type_id'.")
    if atype in ("string", "utf8", "string_view"):
        length = column.max_length or 4000
        return oracledb.DB_TYPE_CLOB if length > 4000 else length
    if atype == "large_string": return oracledb.DB_TYPE_CLOB
    if atype.startswith("int") or atype.startswith("uint") or atype.startswith("float") or atype.startswith("decimal") or atype == "bool":
        return oracledb.DB_TYPE_NUMBER
    if atype.startswith("timestamp"):
        return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP) if column.timezone else oracledb.DB_TYPE_TIMESTAMP
    if atype.startswith("date"): return oracledb.DB_TYPE_DATE
    if atype.startswith("time") or atype.startswith("duration"): return oracledb.DB_TYPE_TIMESTAMP
    if atype in ("binary", "large_binary"): return oracledb.DB_TYPE_BLOB
    if atype in ("list", "large_list", "list_view", "large_list_view", "struct", "map", "json"):
        return getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)
    if atype in ("dictionary", "uuid"): return column.max_length or 4000
    return None


def arrow_to_oracle_ddl(t: pa.DataType, column: Column | None = None) -> str:
    """Map any pa.DataType to its Oracle DDL string. Delegates to map_arrow_to_oracle_ddl when a Column is available."""
    if column is not None:
        return map_arrow_to_oracle_ddl(column)
    return ARROW_TO_ORACLE_DDL.get(pa_type_to_literal(t), "VARCHAR2(255 CHAR)")

ORACLE_TO_ARROW_LITERALS: dict[str, arrow_type_literal] = {
    "DB_TYPE_BINARY_DOUBLE": "float64",
    "DB_TYPE_BINARY_FLOAT": "float32",
    "DB_TYPE_BLOB": "large_binary",
    "DB_TYPE_BOOLEAN": "bool",
    "DB_TYPE_CHAR": "string",
    "DB_TYPE_CLOB": "large_string",
    "DB_TYPE_DATE": "timestamp_s",
    "DB_TYPE_LONG": "large_string",
    "DB_TYPE_LONG_RAW": "large_binary",
    "DB_TYPE_NCHAR": "string",
    "DB_TYPE_NCLOB": "large_string",
    "DB_TYPE_NUMBER": "decimal128", 
    "DB_TYPE_NVARCHAR": "string",
    "DB_TYPE_RAW": "binary",
    "DB_TYPE_TIMESTAMP": "timestamp_ns",
    "DB_TYPE_TIMESTAMP_LTZ": "timestamp_ns",
    "DB_TYPE_TIMESTAMP_TZ": "timestamp_ns",
    "DB_TYPE_VARCHAR": "string",
    "DB_TYPE_VECTOR": "float64",
}

ORACLE_TO_ARROW = {
    "DB_TYPE_BINARY_DOUBLE": ARROW_TYPE["float64"],
    "DB_TYPE_BINARY_FLOAT": ARROW_TYPE["float32"],
    "DB_TYPE_BLOB": ARROW_TYPE["large_binary"],
    "DB_TYPE_BOOLEAN": ARROW_TYPE["bool"],
    "DB_TYPE_CHAR": ARROW_TYPE["string"],
    "DB_TYPE_CLOB": ARROW_TYPE["large_string"],
    "DB_TYPE_DATE": ARROW_TYPE["timestamp_ns"],
    "DB_TYPE_LONG": ARROW_TYPE["large_string"],
    "DB_TYPE_LONG_RAW": ARROW_TYPE["large_binary"],
    "DB_TYPE_NCHAR": ARROW_TYPE["string"],
    "DB_TYPE_NCLOB": ARROW_TYPE["large_string"],
    "DB_TYPE_NUMBER": ARROW_TYPE["decimal128"],
    "DB_TYPE_NVARCHAR": ARROW_TYPE["string"],
    "DB_TYPE_RAW": ARROW_TYPE["binary"],
    "DB_TYPE_TIMESTAMP": ARROW_TYPE["timestamp_ns"],
    "DB_TYPE_TIMESTAMP_LTZ": ARROW_TYPE["timestamp_ns"],
    "DB_TYPE_TIMESTAMP_TZ": ARROW_TYPE["timestamp_ns"],
    "DB_TYPE_VARCHAR": ARROW_TYPE["string"],
    "DB_TYPE_VECTOR": ARROW_TYPE["float64"],
}

def map_python_to_oracle_ddl(column: Column) -> str:
    """Translates a universal FieldModel into an Oracle-specific DDL type string.
       e.g., returns "VARCHAR2(255 CHAR)" or "NUMBER(1, 0)"
    """
    ptype = column.properties.get("python_type", None)
    if ptype is None: raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
    if ptype == "string":
        length = column.max_length or 255
        if length > 4000:
            return "CLOB"
        return f"VARCHAR2({length} CHAR)"
    if ptype == "integer": return "NUMBER"
    if ptype == "float":
        precision = column.precision
        scale = column.scale
        if precision is not None and scale is not None: return f"NUMBER({precision}, {scale})"
        else: return "NUMBER"
    if ptype == "boolean":return "NUMBER(1, 0)"
    if ptype == "datetime": return "TIMESTAMP WITH TIME ZONE" if column.timezone else "TIMESTAMP"
    if ptype == "date": return "DATE"  
    if ptype == "time": return "VARCHAR2(15 CHAR)"
    if ptype == "binary": return "BLOB"
    if ptype == "json": return "JSON"
    return "VARCHAR2(255 CHAR)"

def map_python_to_oracledb_input_size(column: Column) -> Any:
    """Returns the appropriate type hint for oracledb.cursor.setinputsizes().
       This is critical for performance and preventing data truncation during inserts.
    """
    ptype = column.properties.get("python_type", None)
    if ptype is None: raise ValueError(f"Column {column.name} is missing 'python_type' in properties.")
    if ptype == "string":
        length = column.max_length or 4000
        if length > 4000: return oracledb.DB_TYPE_CLOB
        else: return length
    if ptype in ("integer", "float", "boolean"): return oracledb.DB_TYPE_NUMBER
    if ptype == "datetime": return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP)
    if ptype == "date": return oracledb.DB_TYPE_DATE
    if ptype == "binary": return oracledb.DB_TYPE_BLOB
    if ptype == "json":return getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)
    return None


# Legacy mapping.
PythonTypes = Literal[
    "string",
    "integer",
    "float",
    "boolean",
    "datetime", # datetime.datetime # timezone format
    "date",     # datetime.date
    "time",     # datetime.time
    "byte",
    "bytearray",
    "json",     # dict or list
]
def map_oracle_to_python(raw_type: str, scale: int | None = None) -> PythonTypes:
    raw_upper = raw_type.upper()
    if raw_upper in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "ROWID", "UROWID"): return "string"
    if raw_upper == "NUMBER":
        if scale == 0: return "integer" 
        else: return "float"
    if raw_upper in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"): return "float"
    if "TIMESTAMP" in raw_upper: return "datetime"
    if raw_upper == "DATE": return "datetime"
    if raw_upper in ("BLOB", "RAW", "LONG RAW", "BFILE"): return "byte"
    if raw_upper == "JSON": return "json"
    return "string"
