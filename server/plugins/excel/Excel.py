from __future__ import annotations

from typing import Any

from server.plugins.PluginModels import ArrowReader, Catalog, Column, Entity
from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginResponse import PluginResponse

# STRICT BOUNDARY: the Facade only imports the Service. No Engine, no TypeMap.
from .ExcelService import ExcelService


class Excel(Plugin):
    """
    Excel plugin facade. Maps one workbook (.xlsx) to a Catalog and each
    worksheet to an Entity.

    kwargs:
        file_path (str) - path to the .xlsx workbook. Used as the default when
                          the catalog's locator does not carry a namespace.
    """

    service: ExcelService

    def __init__(self, **kwargs: Any) -> None:
        self.service = ExcelService(**kwargs)

    # ------------------------------------------------------------------
    # Data (Record / Row Level)
    # ------------------------------------------------------------------

    def get_data(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            return PluginResponse.success(self.service.get_data(catalog))
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            self.service.create_data(catalog, data)
            return PluginResponse.success(data)
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            self.service.upsert_data(catalog, data)
            return PluginResponse.success(data)
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            self.service.upsert_data(catalog, data)
            return PluginResponse.success(data)
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any) -> PluginResponse[None]:
        try:
            self.service.delete_data(catalog)
            return PluginResponse.success(None)
        except Exception as e:
            return PluginResponse.error(str(e))

    # ------------------------------------------------------------------
    # Catalog (Workbook Level)
    # ------------------------------------------------------------------

    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            return PluginResponse.success(self.service.get_catalog(catalog))
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Excel: workbook creation is not supported via protocol.")

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Excel: workbook updates are not supported via protocol.")

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Excel: workbook upsert is not supported via protocol.")

    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Excel: workbook deletion is destructive. Do it manually.")

    # ------------------------------------------------------------------
    # Entity (Worksheet Level)
    # ------------------------------------------------------------------

    def get_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            result = self.service.get_catalog(catalog)
            if not result.entities:
                return PluginResponse.not_found("No matching worksheet found.")
            return PluginResponse.success(result.entities[0])
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Excel: use create_data to write a new worksheet.")

    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Excel: use upsert_data to overwrite a worksheet.")

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Excel: use upsert_data to overwrite a worksheet.")

    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        try:
            self.service.delete_data(catalog)
            return PluginResponse.success(None)
        except Exception as e:
            return PluginResponse.error(str(e))

    # ------------------------------------------------------------------
    # Column (Field Level) - schema is fixed by worksheet contents
    # ------------------------------------------------------------------

    def get_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            result = self.service.get_catalog(catalog)
            if not result.entities or not result.entities[0].columns:
                return PluginResponse.not_found("No columns found.")
            return PluginResponse.success(result.entities[0].columns[0])
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Excel: worksheet schemas are fixed. Column addition not supported.")

    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Excel: worksheet schemas are fixed. Column updates not supported.")

    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Excel: worksheet schemas are fixed. Column upsert not supported.")

    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Excel: worksheet schemas are fixed. Column deletion not supported.")
