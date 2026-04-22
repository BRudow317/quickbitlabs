"""
ReaderService — orchestrates file discovery, schema inspection, and data I/O.

get_catalog behaviour:
  - Empty catalog  → scan base_path for all supported files (.csv, .parquet, .feather, .arrow)
  - Non-empty      → inspect only the files named in catalog.entities

get_data / write operations expect exactly one entity per catalog call.
Multi-file reads will only succeed when all files share the same schema.

Locator layout stamped on every column:
    plugin          = 'reader'
    namespace       = str(directory)
    entity_name     = filename (e.g. 'sales_2024.parquet')
    additional_locators = {'format': 'csv' | 'parquet' | 'feather'}
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

from server.plugins.PluginModels import Catalog, Column, Entity, Locator
from server.plugins.PluginRegistry import PLUGIN
from server.plugins.readers.CsvEngine import CsvEngine
from server.plugins.readers.FeatherEngine import FeatherEngine
from server.plugins.readers.ParquetEngine import ParquetEngine
from server.plugins.readers.ReaderModels import FORMAT_MAP, SUPPORTED_EXTENSIONS
from server.plugins.readers.ReaderTypeMap import schema_to_columns

logger = logging.getLogger(__name__)

_PLUGIN_NAME: PLUGIN = cast(PLUGIN, "reader")


class ReaderService:

    def __init__(
        self,
        base_path: str | None = None,
        encryption_key: str | None = None,
        **_: Any,
    ) -> None:
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.encryption_key = encryption_key   # base64-encoded 32-byte master key

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_path(self, entity: Entity) -> Path:
        loc = entity.locator
        if loc and loc.namespace and loc.entity_name:
            return Path(loc.namespace) / loc.entity_name
        if loc and loc.entity_name:
            return self.base_path / loc.entity_name
        return self.base_path / entity.name

    def _format_for(self, path: Path, entity: Entity) -> str:
        loc = entity.locator
        if loc and loc.additional_locators:
            fmt = loc.additional_locators.get("format")
            if fmt:
                return str(fmt)
        return FORMAT_MAP.get(path.suffix.lower(), "csv")

    @staticmethod
    def _engine_for(fmt: str):
        if fmt == "parquet":
            return ParquetEngine
        if fmt == "feather":
            return FeatherEngine
        return CsvEngine

    def _build_locator(self, directory: Path, filename: str, fmt: str) -> Locator:
        return Locator(
            plugin=_PLUGIN_NAME,
            namespace=str(directory),
            entity_name=filename,
            additional_locators={"format": fmt},
        )

    # ------------------------------------------------------------------
    # Catalog (discovery / schema inspection)
    # ------------------------------------------------------------------

    def get_catalog(self, catalog: Catalog) -> Catalog:
        if catalog.entities:
            return self._inspect_entities(catalog)
        return self._scan_directory(self.base_path, catalog)

    def _scan_directory(self, directory: Path, catalog: Catalog) -> Catalog:
        entities: list[Entity] = []
        for path in sorted(directory.iterdir()):
            if not path.is_file():
                continue
            fmt = FORMAT_MAP.get(path.suffix.lower())
            if not fmt:
                continue
            try:
                locator = self._build_locator(directory, path.name, fmt)
                schema = self._engine_for(fmt).read_schema(path, master_key_b64=self.encryption_key)
                columns = schema_to_columns(schema, locator)
                entities.append(Entity(name=path.stem, columns=columns))
                logger.debug(f"Reader: discovered {path.name} ({len(columns)} columns)")
            except Exception as exc:
                logger.warning(f"Reader: skipping {path.name} — {exc}")
        return catalog.model_copy(update={"entities": entities})

    def _inspect_entities(self, catalog: Catalog) -> Catalog:
        entities: list[Entity] = []
        for entity in catalog.entities:
            path = self._resolve_path(entity)
            if not path.exists():
                logger.warning(f"Reader: file not found — {path}")
                continue
            fmt = self._format_for(path, entity)
            try:
                locator = self._build_locator(path.parent, path.name, fmt)
                schema = self._engine_for(fmt).read_schema(path, master_key_b64=self.encryption_key)
                columns = schema_to_columns(schema, locator)
                entities.append(Entity(name=entity.name or path.stem, columns=columns))
            except Exception as exc:
                logger.warning(f"Reader: skipping {path.name} — {exc}")
        return catalog.model_copy(update={"entities": entities})

    # ------------------------------------------------------------------
    # Data operations
    # ------------------------------------------------------------------

    def get_data(self, catalog: Catalog) -> pa.RecordBatchReader:
        """
        Read data from catalog entities into a single RecordBatchReader.
        All entities must share the same schema when there are multiple.
        """
        streams: list[pa.RecordBatchReader] = []
        for entity in catalog.entities:
            path = self._resolve_path(entity)
            fmt = self._format_for(path, entity)
            streams.append(self._engine_for(fmt).read(path, master_key_b64=self.encryption_key))

        if not streams:
            raise ValueError("Catalog contains no readable entities.")
        if len(streams) == 1:
            return streams[0]

        schema = streams[0].schema

        def _all_batches():
            for s in streams:
                yield from s

        return pa.RecordBatchReader.from_batches(schema, _all_batches())

    def create_data(self, catalog: Catalog, data: pa.RecordBatchReader) -> None:
        """Write *data* to a new file. Raises FileExistsError if the target already exists."""
        entity, path, fmt = self._single_entity(catalog)
        if path.exists():
            raise FileExistsError(f"File already exists: {path}. Use upsert_data to overwrite.")
        self._engine_for(fmt).write(path, data, master_key_b64=self.encryption_key)
        logger.info(f"Reader: created {path}")

    def upsert_data(self, catalog: Catalog, data: pa.RecordBatchReader) -> None:
        """Write *data*, overwriting any existing file at the target path."""
        entity, path, fmt = self._single_entity(catalog)
        self._engine_for(fmt).write(path, data, master_key_b64=self.encryption_key)
        logger.info(f"Reader: upserted {path}")

    def delete_data(self, catalog: Catalog) -> None:
        """Delete file(s) and any associated .rkey sidecars referenced in the catalog."""
        for entity in catalog.entities:
            path = self._resolve_path(entity)
            if path.exists():
                path.unlink()
                sidecar = path.with_name(path.name + ".rkey")
                if sidecar.exists():
                    sidecar.unlink()
                logger.info(f"Reader: deleted {path}")
            else:
                logger.warning(f"Reader: file not found for deletion — {path}")

    # ------------------------------------------------------------------

    def _single_entity(self, catalog: Catalog) -> tuple[Entity, Path, str]:
        """Extract the single target entity from a write catalog."""
        if not catalog.entities:
            raise ValueError("Catalog must contain at least one entity for write operations.")
        entity = catalog.entities[0]
        path = self._resolve_path(entity)
        fmt = self._format_for(path, entity)
        return entity, path, fmt
