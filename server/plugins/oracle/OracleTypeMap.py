from typing import Any
import oracledb

from server.models.PluginModels import PythonTypes, FieldModel

# ---------------------------------------------------------
# 1. Oracle -> Python (For generating the Contract during Discovery)
# ---------------------------------------------------------

def map_oracle_to_python(raw_type: str, scale: int | None = None) -> PythonTypes:
    """
    Translates an Oracle catalog DATA_TYPE into the universal PythonTypes literal.
    """
    raw_upper = raw_type.upper()

    if raw_upper in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "ROWID", "UROWID"):
        return "string"
        
    if raw_upper == "NUMBER":
        # Oracle NUMBER can be an integer or a float. We check the scale.
        # Scale 0 means no decimals (Integer). Anything else is a Float.
        if scale == 0:
            return "integer"
        return "float"
        
    if raw_upper in ("FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return "float"
        
    if "TIMESTAMP" in raw_upper:
        return "datetime"
        
    if raw_upper == "DATE":
        # Oracle DATE actually contains time down to the second
        return "datetime"
        
    if raw_upper in ("BLOB", "RAW", "LONG RAW", "BFILE"):
        return "binary"
        
    if raw_upper == "JSON":
        return "json"

    # Default fallback
    return "string"


# ---------------------------------------------------------
# 2. Python -> Oracle DDL (For preparing Target schemas)
# ---------------------------------------------------------

def map_python_to_oracle_ddl(field: FieldModel) -> str:
    """
    Translates a universal FieldModel into an Oracle-specific DDL type string.
    e.g., returns "VARCHAR2(255 CHAR)" or "NUMBER(1, 0)"
    """
    ptype = field.python_type
    
    if ptype == "string":
        # Oracle maxes out at 4000 bytes for standard VARCHAR2.
        length = field.length or 255
        if length > 4000:
            return "CLOB"
        return f"VARCHAR2({length} CHAR)"
        
    if ptype == "integer":
        return "NUMBER"
        
    if ptype == "float":
        precision = field.precision
        scale = field.scale
        if precision is not None and scale is not None:
            return f"NUMBER({precision}, {scale})"
        return "NUMBER"
        
    if ptype == "boolean":
        # Oracle < 23c does not have a native BOOLEAN column type
        return "NUMBER(1, 0)"
        
    if ptype == "datetime":
        return "TIMESTAMP WITH TIME ZONE" if field.timezone else "TIMESTAMP"
        
    if ptype == "date":
        return "DATE"
        
    if ptype == "time":
        # Oracle doesn't have a pure TIME type, often mapped to a string or INTERVAL
        return "VARCHAR2(15 CHAR)"
        
    if ptype == "binary":
        return "BLOB"
        
    if ptype == "json":
        # In 21c+, JSON is a native type. For broader compatibility, CLOB with IS JSON is common,
        # but we'll assume modern JSON type or let the Engine handle constraints.
        return "JSON"

    return "VARCHAR2(255 CHAR)"


# ---------------------------------------------------------
# 3. Python -> oracledb Bind Types (For the Engine's execution layer)
# ---------------------------------------------------------

def map_field_to_oracledb_input_size(field: FieldModel) -> Any:
    """
    Returns the appropriate type hint for oracledb.cursor.setinputsizes().
    This is critical for performance and preventing data truncation during inserts.
    """
    ptype = field.python_type
    
    if ptype == "string":
        # For strings <= 4000, passing the integer length is best practice for oracledb
        length = field.length or 4000
        if length > 4000:
            return oracledb.DB_TYPE_CLOB
        return length
        
    if ptype in ("integer", "float", "boolean"):
        return oracledb.DB_TYPE_NUMBER
        
    if ptype == "datetime":
        return getattr(oracledb, "DB_TYPE_TIMESTAMP_TZ", oracledb.DB_TYPE_TIMESTAMP)
        
    if ptype == "date":
        return oracledb.DB_TYPE_DATE
        
    if ptype == "binary":
        return oracledb.DB_TYPE_BLOB
        
    if ptype == "json":
        return getattr(oracledb, "DB_TYPE_JSON", oracledb.DB_TYPE_CLOB)

    return None