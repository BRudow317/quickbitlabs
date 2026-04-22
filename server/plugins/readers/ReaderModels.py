from __future__ import annotations

from pathlib import Path

SUPPORTED_EXTENSIONS: set[str] = {'.csv', '.parquet', '.feather', '.arrow'}

# Maps file extension → format token used throughout the plugin
FORMAT_MAP: dict[str, str] = {
    '.csv':     'csv',
    '.parquet': 'parquet',
    '.feather': 'feather',
    '.arrow':   'feather',
}
