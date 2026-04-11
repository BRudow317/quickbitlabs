from typing import Any
import oracledb
from server.plugins.PluginModels import PythonTypes, Column, arrow_types, arrow_type_literal
from typing import Any
import oracledb

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

def map_arrow_to_oracle_ddl(column: Column) -> str:
    atype = column.arrow_type_id
    if atype is None: raise ValueError(f"Column {column.name} is missing 'arrow_type_id'.")
    if atype in ("string", "utf8"):
        length = column.max_length or 255
        if length > 4000:
            return "CLOB"
        return f"VARCHAR2({length} CHAR)"
    if atype == "large_string": return "CLOB"
    if atype.startswith("int") or atype.startswith("uint"): return "NUMBER"
    if atype.startswith("float") or atype.startswith("decimal"):
        return f"NUMBER({column.precision}, {column.scale})" if column.precision and column.scale else "NUMBER"
    if atype == "bool": return "NUMBER(1, 0)"
    if atype.startswith("timestamp"): return "TIMESTAMP WITH TIME ZONE" if column.timezone else "TIMESTAMP"
    if atype.startswith("date"): return "DATE"
    if atype.startswith("time"): return "TIMESTAMP"
    if atype in ("binary", "large_binary"): return "BLOB"
    return "VARCHAR2(255 CHAR)"

def map_column_to_oracledb_input_size(column: Column) -> Any:
    atype = column.arrow_type_id
    if atype is None: raise ValueError(f"Column {column.name} is missing 'arrow_type_id'.")
    if atype in ("string", "utf8"):
        length = column.max_length or 4000
        if length > 4000:
            return oracledb.DB_TYPE_CLOB
        return length
    if atype == "large_string": return oracledb.DB_TYPE_CLOB
    if atype.startswith("int") or atype.startswith("uint") or atype.startswith("float") or atype.startswith("decimal") or atype == "bool": return oracledb.DB_TYPE_NUMBER
    if atype.startswith("timestamp"): return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP) if column.timezone else getattr(oracledb, "DB_TYPE_TIMESTAMP")
    if atype.startswith("date"): return oracledb.DB_TYPE_DATE
    if atype.startswith("time"): return getattr(oracledb, "DB_TYPE_TIMESTAMP")
    if atype in ("binary", "large_binary"): return oracledb.DB_TYPE_BLOB
    return None

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
    "DB_TYPE_BINARY_DOUBLE": arrow_types["float64"],
    "DB_TYPE_BINARY_FLOAT": arrow_types["float32"],
    "DB_TYPE_BLOB": arrow_types["large_binary"],
    "DB_TYPE_BOOLEAN": arrow_types["bool"],
    "DB_TYPE_CHAR": arrow_types["string"],
    "DB_TYPE_CLOB": arrow_types["large_string"],
    "DB_TYPE_DATE": arrow_types["timestamp_ns"],
    "DB_TYPE_LONG": arrow_types["large_string"],
    "DB_TYPE_LONG_RAW": arrow_types["large_binary"],
    "DB_TYPE_NCHAR": arrow_types["string"],
    "DB_TYPE_NCLOB": arrow_types["large_string"],
    "DB_TYPE_NUMBER": arrow_types["decimal128"],
    "DB_TYPE_NVARCHAR": arrow_types["string"],
    "DB_TYPE_RAW": arrow_types["binary"],
    "DB_TYPE_TIMESTAMP": arrow_types["timestamp_ns"],
    "DB_TYPE_TIMESTAMP_LTZ": arrow_types["timestamp_ns"],
    "DB_TYPE_TIMESTAMP_TZ": arrow_types["timestamp_ns"],
    "DB_TYPE_VARCHAR": arrow_types["string"],
    "DB_TYPE_VECTOR": arrow_types["float64"],
}

def map_oracle_to_python(raw_type: str, scale: int | None = None) -> PythonTypes:
    """Translates an Oracle catalog DATA_TYPE into the universal PythonTypes literal."""
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
