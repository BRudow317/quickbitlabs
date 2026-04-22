"""
ExcelService — orchestrates sheet discovery, schema inspection, and data I/O.

Catalog model:
    Catalog  ↔  one Excel workbook (.xlsx)
    Entity   ↔  one worksheet within that workbook

Locator layout stamped on every column:
    plugin       = 'excel'
    namespace    = str(path_to_workbook)   — full absolute path to the .xlsx file
    entity_name  = sheet_name

get_catalog behaviour:
  - Empty catalog  → list all worksheets in the workbook and inspect each
  - Non-empty      → inspect only the sheets named by catalog.entities

Write operations (create_data / upsert_data) target the first entity in the catalog.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

from server.plugins.PluginModels import Catalog, Entity, Locator
from server.plugins.PluginRegistry import PLUGIN
from server.plugins.excel.ExcelEngine import ExcelEngine
from server.plugins.excel.ExcelTypeMap import schema_to_columns

logger = logging.getLogger(__name__)

_PLUGIN_NAME: PLUGIN = cast(PLUGIN, "excel")


class ExcelService:

    def __init__(self, file_path: str | None = None, **_: Any) -> None:
        self.file_path = Path(file_path) if file_path else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_file(self, catalog: Catalog) -> Path:
        """Derive the workbook path from the catalog's first locator, then the service default."""
        if catalog.entities:
            loc = catalog.entities[0].locator
            if loc and loc.namespace:
                return Path(loc.namespace)
        if self.file_path:
            return self.file_path
        raise ValueError(
            "No Excel file path specified. "
            "Pass file_path to the plugin constructor or set locator.namespace."
        )

    def _sheet_from_entity(self, entity: Entity) -> tuple[Path, str]:
        """Return (workbook_path, sheet_name) for an entity."""
        loc = entity.locator
        if loc and loc.namespace and loc.entity_name:
            return Path(loc.namespace), loc.entity_name
        if self.file_path:
            return self.file_path, entity.name
        raise ValueError(f"Cannot resolve workbook path for entity '{entity.name}'")

    def _build_locator(self, workbook_path: Path, sheet_name: str) -> Locator:
        return Locator(
            plugin=_PLUGIN_NAME,
            namespace=str(workbook_path),
            entity_name=sheet_name,
        )

    # ------------------------------------------------------------------
    # Catalog (discovery / schema inspection)
    # ------------------------------------------------------------------

    def get_catalog(self, catalog: Catalog) -> Catalog:
        path = self._resolve_file(catalog)
        if catalog.entities:
            return self._inspect_sheets(path, catalog)
        return self._scan_workbook(path, catalog)

    def _scan_workbook(self, path: Path, catalog: Catalog) -> Catalog:
        """Discover and inspect every worksheet in the workbook."""
        entities: list[Entity] = []
        for sheet_name in ExcelEngine.list_sheets(path):
            try:
                locator = self._build_locator(path, sheet_name)
                schema = ExcelEngine.read_schema(path, sheet_name)
                columns = schema_to_columns(schema, locator)
                entities.append(Entity(name=sheet_name, columns=columns))
                logger.debug(f"Excel: discovered sheet '{sheet_name}' ({len(columns)} columns)")
            except Exception as exc:
                logger.warning(f"Excel: skipping sheet '{sheet_name}' — {exc}")
        return catalog.model_copy(update={"entities": entities})

    def _inspect_sheets(self, path: Path, catalog: Catalog) -> Catalog:
        """Inspect only the sheets named in catalog.entities."""
        entities: list[Entity] = []
        for entity in catalog.entities:
            _, sheet_name = self._sheet_from_entity(entity)
            try:
                locator = self._build_locator(path, sheet_name)
                schema = ExcelEngine.read_schema(path, sheet_name)
                columns = schema_to_columns(schema, locator)
                entities.append(Entity(name=sheet_name, columns=columns))
            except Exception as exc:
                logger.warning(f"Excel: skipping sheet '{sheet_name}' — {exc}")
        return catalog.model_copy(update={"entities": entities})

    # ------------------------------------------------------------------
    # Data operations
    # ------------------------------------------------------------------

    def get_data(self, catalog: Catalog) -> pa.RecordBatchReader:
        """
        Read data from catalog sheets into a single RecordBatchReader.
        All sheets must share the same schema when there are multiple entities.
        """
        streams: list[pa.RecordBatchReader] = []
        for entity in catalog.entities:
            path, sheet_name = self._sheet_from_entity(entity)
            streams.append(ExcelEngine.read(path, sheet_name))

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
        """Write *data* to a new worksheet. Raises if the sheet already exists."""
        path, sheet_name = self._single_target(catalog)
        if path.exists() and sheet_name in ExcelEngine.list_sheets(path):
            raise FileExistsError(
                f"Sheet '{sheet_name}' already exists in {path}. Use upsert_data to overwrite."
            )
        ExcelEngine.write_sheet(path, sheet_name, data)
        logger.info(f"Excel: created sheet '{sheet_name}' in {path}")

    def upsert_data(self, catalog: Catalog, data: pa.RecordBatchReader) -> None:
        """Write *data*, overwriting the worksheet if it already exists."""
        path, sheet_name = self._single_target(catalog)
        ExcelEngine.write_sheet(path, sheet_name, data)
        logger.info(f"Excel: upserted sheet '{sheet_name}' in {path}")

    def delete_data(self, catalog: Catalog) -> None:
        """Remove worksheet(s) from the workbook."""
        for entity in catalog.entities:
            path, sheet_name = self._sheet_from_entity(entity)
            ExcelEngine.delete_sheet(path, sheet_name)
            logger.info(f"Excel: deleted sheet '{sheet_name}' from {path}")

    # ------------------------------------------------------------------

    def _single_target(self, catalog: Catalog) -> tuple[Path, str]:
        """Extract the single write target from a catalog."""
        if not catalog.entities:
            raise ValueError("Catalog must contain at least one entity for write operations.")
        return self._sheet_from_entity(catalog.entities[0])
