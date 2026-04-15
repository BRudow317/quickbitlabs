from pathlib import Path
import polars as pl

# Regex breakdown:
# \b(?!\d)       -> Ensure we start at a boundary (no digit before)
# \d{3}          -> 3 Area digits
# [- .\\/_]?     -> Optional delimiter: dash, space, dot, backslash, underscore, or slash
# \d{2}          -> 2 Group digits
# [- .\\/_]?     -> Optional delimiter again
# \d{4}          -> 4 Serial digits
# (?!\d)\b       -> Ensure we end at a boundary (no digit after)
 
INPUT  = Path(r"C:\path\to\input.csv")
OUTPUT = Path(r"C:\path\to\output_redacted.csv")
 
SSN_RE = (
    r"(?i)"
    r"(?:"
    r"(?:SSN|S\.S\.N\.?|Social\s+Security(?:\s+Number)?|SS#|SSN#)\s*[:#]?\s*\d{3}[-\s.]?\d{2}[-\s.]?\d{4}"
    r"|\b\d{3}[-\s.]\d{2}[-\s.]\d{4}\b"
    r"|\b\d{9}\b"
    r")"
)
 
(
    pl.scan_csv(INPUT, infer_schema_length=0, encoding="utf8-lossy")
    .with_columns(
        pl.col("subject").str.replace_all(SSN_RE, "[REDACTED]"),
        pl.col("description").str.replace_all(SSN_RE, "[REDACTED]"),
    )
    .sink_csv(OUTPUT)
)
