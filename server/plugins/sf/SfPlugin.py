from __future__ import annotations

import csv
import io
from typing import Any

import pyarrow as pa

from server.plugins.PluginProtocol import Plugin
from server.plugins.sf.engines.SfClient import SfClient
from server.plugins.sf.engines.SfRestEngine import SfRest
from server.plugins.sf.engines.SfBulk2Engine import Bulk2
from server.plugins.sf.models.SfTypeMap import cast_record
from server.plugins.PluginModels import ArrowStream, Entity, Column, Catalog, Records
from server.plugins.PluginResponse import PluginResponse

import logging
logger = logging.getLogger(__name__)


class SfPlugin:
    """
    Facade Interface for higher level Salesforce operations.
    """
    client: SfClient
    catalog: Catalog
    _rest: SfRest | None = None
    _bulk2: Bulk2 | None = None

    def __init__(
        self,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        org_url: str | None = None,
        catalog: Catalog | None = None,
    ):
        self.client = SfClient.create(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            base_url=org_url,
        )
        self.catalog = catalog if catalog is not None else Catalog(name='salesforce')

    @property
    def rest(self) -> SfRest:
        if self._rest is None:
            self._rest = SfRest(self.client)
        return self._rest

    @property
    def bulk2(self) -> Bulk2:
        if self._bulk2 is None:
            self._bulk2 = Bulk2(self.client)
        return self._bulk2

    # Connector specifics
    def query(self, soql: str, object_name: str | None = None) -> Records:
        """REST SOQL query. Best for small, real-time result sets."""
        field_types: dict[str, str] = {}
        if object_name:
            result = self._describe_entity(object_name)
            if result.ok and result.data:
                field_types = {c.name: c.raw_type for c in result.data.columns if c.raw_type}
        for record in self.rest.query_all_iter(soql):
            clean = {k: v for k, v in record.items() if k != 'attributes'}
            yield cast_record(clean, field_types) if field_types else clean

    def bulk_query(self, soql: str, object_name: str | None = None) -> Records:
        """Bulk2 SOQL query. Best for large exports."""
        field_types: dict[str, str] = {}
        if object_name:
            result = self._describe_entity(object_name)
            if result.ok and result.data:
                field_types = {c.name: c.raw_type for c in result.data.columns if c.raw_type}
        for csv_page in self.bulk2.query(soql):
            for record in csv.DictReader(io.StringIO(csv_page.decode())):
                yield cast_record(record, field_types) if field_types else dict(record)

    def get_limits(self) -> dict[str, Any]:
        return self.rest.limits()

# ============================ INTERNAL ============================

    def _describe_entity(self, name: str) -> PluginResponse[Entity | None]:
        """Describe a Salesforce object by name; caches result on self.catalog."""
        try:
            if name not in self.catalog.entity_map:
                describe = getattr(self.rest, name).describe()
                columns = []
                for f in describe.get('fields', []):
                    sf_type = f['type']
                    if sf_type in ('address', 'location'):
                        logger.debug(f"Skipping compound field {f['name']} ({sf_type}) on {name}")
                        continue
                    pv = f.get('picklistValues') or []
                    columns.append(Column(
                        name=f['name'],
                        raw_type=sf_type,
                        primary_key=(f['name'] == 'Id'),
                        is_nullable=f.get('nillable', True),
                        is_unique=f.get('unique', False),
                        max_length=f.get('length') or None,
                        precision=f.get('precision') or None,
                        scale=f.get('scale') or None,
                        is_read_only=not f.get('updateable', True),
                        default_value=f.get('defaultValue'),
                        enum_values=[v['value'] for v in pv if v.get('active')],
                    ))
                self.catalog.entities.append(Entity(name=name, columns=columns))

            return PluginResponse.success(data=self.catalog.entity_map[name])
        except Exception as e:
            return PluginResponse.error(str(e))

# ============================ METADATA ============================

    # CATALOG
    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog | None]:
        """
        Describe Salesforce objects and return a populated Catalog.

        If catalog has entities, describes only those. Otherwise discovers all migratable objects.
        """
        try:
            names = [e.name for e in catalog.entities] if catalog.entities else None

            if names is None:
                names = [obj['name'] for obj in self.rest.describe_migratable()]

            entities = []
            for name in names:
                result = self._describe_entity(name)
                if result.ok and result.data:
                    entities.append(result.data)
                else:
                    logger.warning(f"Skipping {name}: {result.message}")

            catalog.entities = entities
            self.catalog = catalog
            return PluginResponse.success(data=catalog)
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog | None]:
        return PluginResponse.not_implemented("Salesforce does not support catalog creation via API")

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog | None]:
        return PluginResponse.not_implemented("Salesforce does not support catalog modification via API")

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog | None]:
        try:
            for entity in catalog.entities:
                pass
            return PluginResponse.success(data=catalog)
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Salesforce does not support catalog deletion via API")


    # ENTITY
    def get_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity | None]:
        """Describe the first entity in catalog. Populates columns in place."""
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog contains no entities to describe")
            entity = catalog.entities[0]
            result = self._describe_entity(entity.name)
            if result.ok and result.data:
                entity.columns = result.data.columns
            return result
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity | None]:
        return PluginResponse.not_implemented("Salesforce does not support entity creation via data API")

    def update_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity | None]:
        return PluginResponse.not_implemented("Salesforce does not support entity modification via data API")

    def upsert_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity | None]:
        return PluginResponse.not_implemented("Salesforce does not support entity creation via data API")

    def delete_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Salesforce does not support entity deletion via data API")


    # COLUMN
    def get_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column | None]:
        """Describe the first entity, then return metadata for the first column in place."""
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.error("Catalog must contain an entity with at least one column")
            entity = catalog.entities[0]
            target = entity.columns[0]
            entity_result = self._describe_entity(entity.name)
            if not entity_result.ok or not entity_result.data:
                return PluginResponse.not_found(f"Entity {entity.name} not found")
            col = entity_result.data.column_map.get(target.name)
            if not col:
                return PluginResponse.not_found(f"{entity.name}.{target.name} not found")
            target.raw_type = col.raw_type
            target.primary_key = col.primary_key
            target.is_nullable = col.is_nullable
            target.is_unique = col.is_unique
            target.max_length = col.max_length
            target.precision = col.precision
            target.scale = col.scale
            target.is_read_only = col.is_read_only
            target.default_value = col.default_value
            target.enum_values = col.enum_values
            return PluginResponse.success(data=target)
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column | None]:
        return PluginResponse.not_implemented("Salesforce does not support field creation via data API")

    def update_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column | None]:
        return PluginResponse.not_implemented("Salesforce does not support field modification via data API")

    def upsert_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column | None]:
        return PluginResponse.not_implemented("Salesforce does not support field creation via data API")

    def delete_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[None]:
        return PluginResponse.not_implemented("Salesforce does not support field deletion via data API")

# ============================ DATA ============================

    def get_data(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[ArrowStream | None]:
        """Bulk2 full extract. The default read path."""
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog contains no entities")
            name = catalog.entities[0].name
            entity_result = self._describe_entity(name)
            if not entity_result.ok or not entity_result.data:
                return PluginResponse.not_found(f"Entity {name} not found")
            fields = ', '.join(c.name for c in entity_result.data.columns)
            return PluginResponse.success(data=self.bulk_query(f"SELECT {fields} FROM {name}", name))
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> PluginResponse[ArrowStream | None]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog contains no entities")
            name = catalog.entities[0].name
            table = data.read_all()
            results = getattr(self.bulk2, name).insert(table)
            return PluginResponse.success(data=results)
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> PluginResponse[ArrowStream | None]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog contains no entities")
            name = catalog.entities[0].name
            table = data.read_all()
            results = getattr(self.bulk2, name).update(table)
            return PluginResponse.success(data=results)
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> PluginResponse[ArrowStream | None]:
        external_id = kwargs.get('external_id_field', 'Id')
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog contains no entities")
            name = catalog.entities[0].name
            table = data.read_all()
            results = getattr(self.bulk2, name).upsert(table, external_id_field=external_id)
            return PluginResponse.success(data=results)
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_data(self, catalog: Catalog, data: ArrowStream, **kwargs: Any) -> PluginResponse[None]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog contains no entities")
            name = catalog.entities[0].name
            table = data.read_all()
            getattr(self.bulk2, name).delete(table)
            return PluginResponse.success(data=None)
        except Exception as e:
            return PluginResponse.error(str(e))
