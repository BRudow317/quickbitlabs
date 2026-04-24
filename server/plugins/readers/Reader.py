from __future__ import annotations

from typing import Any

from server.plugins.PluginModels import ArrowReader, Catalog, Column, Entity
from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginResponse import PluginResponse

# STRICT BOUNDARY: the Facade only imports the Service. No Engines, no Encryption.
from .ReaderService import ReaderService


class Reader(Plugin):
    """
    File-reader plugin facade. Reads and writes CSV, Parquet, and Feather files.
    kwargs:
        base_path (str)      — directory to scan when get_catalog receives an empty catalog.
        encryption_key (str) — base64-encoded 32-byte AES master key; enables encryption at rest.
    """

    service: ReaderService

    def __init__(self, **kwargs: Any) -> None:
        self.service = ReaderService(**kwargs)

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
    # Catalog (Directory / Dataset Level)
    # ------------------------------------------------------------------

    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        try:
            return PluginResponse.success(self.service.get_catalog(catalog))
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Reader: directory creation is not supported via protocol.")

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Reader: directory updates are not supported via protocol.")

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Reader: directory upsert is not supported via protocol.")

    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Reader: directory deletion is destructive. Do it manually.")

    # ------------------------------------------------------------------
    # Entity (File Level)
    # ------------------------------------------------------------------

    def get_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            result = self.service.get_catalog(catalog)
            if not result.entities:
                return PluginResponse.not_found("No matching file found.")
            return PluginResponse.success(result.entities[0])
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Reader: use create_data to write a new file.")

    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Reader: use upsert_data to overwrite a file.")

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Reader: use upsert_data to overwrite a file.")

    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        try:
            self.service.delete_data(catalog)
            return PluginResponse.success(None)
        except Exception as e:
            return PluginResponse.error(str(e))

    # ------------------------------------------------------------------
    # Column (Field Level) — schema is fixed by the file on disk
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
        return PluginResponse.not_implemented("Reader: file schemas are fixed. Column addition not supported.")

    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Reader: file schemas are fixed. Column updates not supported.")

    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Reader: file schemas are fixed. Column upsert not supported.")

    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Reader: file schemas are fixed. Column deletion not supported.")
