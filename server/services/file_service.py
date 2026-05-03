"""
FileService - processes raw file uploads through the Reader or Excel plugin,
converts each entity to encrypted Parquet for persistent storage, and returns
a Catalog (with 'reader' locators pointing at the saved Parquet files) alongside
a PyArrow Table for preview slicing.

Storage layout:  server/uploads/<username>/<original_stem>__<entity_name>.parquet

Because every saved file uses the 'reader' plugin locator, the standard
POST /api/data/ endpoint can serve previews and queries without extra wiring.
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import cast

import pyarrow as pa

from server.plugins.PluginModels import Catalog, Entity, Locator
from server.plugins.PluginRegistry import PLUGIN, get_plugin
from server.plugins.readers.ReaderModels import FORMAT_MAP
from server.plugins.readers.ReaderService import ReaderService
from server.plugins.readers.ParquetEngine import ParquetEngine

logger = logging.getLogger(__name__)

_UPLOADS_DIR = Path(__file__).resolve().parent.parent / "uploads"
_EXCEL_EXTS  = {".xlsx", ".xls", ".xlsm", ".xlsb"}
_READER_EXTS = {".csv", ".parquet", ".feather", ".arrow"}


def _upload_dir(username: str) -> Path:
    d = _UPLOADS_DIR / username
    d.mkdir(parents=True, exist_ok=True)
    return d


def process_upload(
    file_bytes: bytes,
    filename: str,
    username: str,
    encryption_key: str | None = None,
) -> tuple[Catalog, pa.Table]:
    """
    Persist *file_bytes* (uploaded as *filename*) through the appropriate plugin,
    save each entity as an encrypted Parquet file in the user's upload directory,
    and return a Catalog (reader-plugin locators → saved files) plus the first
    entity's PyArrow Table for preview slicing.

    Raises ValueError for unsupported extensions or empty files.
    Raises RuntimeError if the plugin fails to read the file.
    """
    ext = Path(filename).suffix.lower()
    stem = Path(filename).stem
    upload_dir = _upload_dir(username)

    with tempfile.TemporaryDirectory() as staging:
        stage_path = Path(staging) / filename
        stage_path.write_bytes(file_bytes)

        if ext in _EXCEL_EXTS:
            raw_catalog, tables = _read_excel(stage_path)
        elif ext in _READER_EXTS:
            raw_catalog, tables = _read_single_file(staging)
        else:
            raise ValueError(f"Unsupported file type: '{ext}'. Supported: csv, parquet, feather, arrow, xlsx")

    if not raw_catalog.entities:
        raise ValueError(f"No data found in '{filename}'")

    saved_entities: list[Entity] = []
    all_tables: list[pa.Table] = []

    for entity, table in zip(raw_catalog.entities, tables):
        safe_name = entity.name.replace(" ", "_").replace("/", "_")
        parquet_name = f"{stem}__{safe_name}.parquet"
        parquet_path = upload_dir / parquet_name

        ParquetEngine.write(
            parquet_path,
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
            master_key_b64=encryption_key,
        )
        logger.info(f"FileService: saved '{parquet_name}' ({len(table)} rows) for user '{username}'")

        locator = Locator(
            plugin=cast(PLUGIN, "reader"),
            namespace=str(upload_dir),
            entity_name=parquet_name,
            is_file=True,
            additional_locators={"format": "parquet"},
        )
        saved_columns = [col.model_copy(update={"locator": locator}) for col in entity.columns]
        saved_entities.append(Entity(
            name=entity.name,
            entity_type="file",
            plugin=cast(PLUGIN, "reader"),
            namespace=str(upload_dir),
            row_count_estimate=len(table),
            columns=saved_columns,
        ))
        all_tables.append(table)

    final_catalog = Catalog(
        entities=saved_entities,
        source_type=cast(PLUGIN, "reader"),
    )

    return final_catalog, all_tables[0]


# ================================================---------
# Internal readers
# ================================================---------

def _read_excel(path: Path) -> tuple[Catalog, list[pa.Table]]:
    """Read all worksheets from an Excel file via the excel plugin."""
    plugin = get_plugin("excel", file_path=str(path))
    resp = plugin.get_catalog(Catalog())
    if not resp.ok:
        raise RuntimeError(f"Excel plugin catalog: {resp.message}")

    catalog = resp.data
    if catalog is None:
        raise ValueError("No readable entities found in the Excel file")
    tables: list[pa.Table] = []

    for entity in catalog.entities:
        dr = plugin.get_data(catalog.model_copy(update={"entities": [entity]}))
        if not dr.ok or dr.data is None:
            raise RuntimeError(f"Excel sheet '{entity.name}': {dr.message}")
        tables.append(dr.data.read_all())

    return catalog, tables


def _read_single_file(staging_dir: str) -> tuple[Catalog, list[pa.Table]]:
    """Read a single csv / parquet / feather file via the reader plugin."""
    plugin = get_plugin("reader", base_path=staging_dir)
    resp = plugin.get_catalog(Catalog())
    if not resp.ok:
        raise RuntimeError(f"Reader plugin catalog: {resp.message}")

    catalog = resp.data
    if not catalog or not catalog.entities:
        # _scan_directory swallows parse errors — re-read directly to surface the real cause.
        _raise_parse_error(Path(staging_dir))
        raise ValueError("No readable entities found in the uploaded file")

    tables: list[pa.Table] = []
    for entity in catalog.entities:
        dr = plugin.get_data(catalog.model_copy(update={"entities": [entity]}))
        if not dr.ok:
            raise RuntimeError(f"Reader file '{entity.name}': {dr.message}")
        if dr.data is None:
            raise RuntimeError(f"Reader file '{entity.name}': no data returned")
        tables.append(dr.data.read_all())

    return catalog, tables


def _raise_parse_error(staging_dir: Path) -> None:
    """Attempt a direct schema read on each file in staging_dir to surface parse errors."""
    for f in staging_dir.iterdir():
        fmt = FORMAT_MAP.get(f.suffix.lower())
        if not fmt:
            continue
        try:
            ReaderService._engine_for(fmt).read_schema(f)
        except Exception as exc:
            raise ValueError(f"'{f.name}' could not be parsed: {exc}") from exc
