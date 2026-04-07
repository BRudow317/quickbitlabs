from __future__ import annotations

import csv
import io
from typing import Any

from server.plugins.PluginProtocol import Plugin
from server.plugins.sf.engines.SfClient import SfClient
from server.plugins.sf.engines.SfRestEngine import SfRest
from server.plugins.sf.engines.SfBulk2Engine import Bulk2
from server.plugins.sf.models.SfTypeMap import SF_TYPE_MAP, cast_record, prepare_record
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
        client: SfClient = SfClient.create(
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
            result = self.get_table(object_name)
            if result.ok and result.data:
                field_types = {c.name: c.raw_type for c in result.data.fields if c.raw_type}
        for record in self.rest.query_all_iter(soql):
            clean = {k: v for k, v in record.items() if k != 'attributes'}
            yield cast_record(clean, field_types) if field_types else clean

    def bulk_query(self, soql: str, object_name: str | None = None) -> Records:
        """Bulk2 SOQL query. Best for large exports."""
        field_types: dict[str, str] = {}
        if object_name:
            result = self.get_table(object_name)
            if result.ok and result.data:
                field_types = {c.name: c.raw_type for c in result.data.fields if c.raw_type}
        for csv_page in self.bulk2.query(soql):
            for record in csv.DictReader(io.StringIO(csv_page)):
                yield cast_record(record, field_types) if field_types else dict(record)

    def get_limits(self) -> dict[str, Any]:
        return self.rest.limits()

# ============================ METADATA ============================
    
    # CATALOG
    def get_catalog(self, catalog: Catalog | str | None = None, **kwargs: Any) -> PluginResponse[Catalog]:
        """
        Describe Salesforce objects and return a populated Catalog.

        Uses the REST describe API throughout: global describe to discover migratable
        objects, then one individual sobject describe per object for field metadata.

        Full discovery (records=None): global describe → filter migratable → per-object describes.
        Specific records: one individual describe per named object.
        """
        try:
            if isinstance(catalog, Catalog):
                records = [t.name for t in catalog.entities]
                target = catalog
            else:
                records = kwargs.get('records')
                target = Catalog(name=catalog or 'salesforce')

            if records is None:
                records = [obj['name'] for obj in self.rest.describe_migratable()]

            entities = []
            for s in records:
                result = self.get_table(s)
                if result.ok and result.data:
                    entities.append(result.data)
                else:
                    logger.warning(f"Skipping {s}: {result.message}")

            target.entities = entities
            self.catalog = target
            return PluginResponse.success(data=target)
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Salesforce does not support catalog creation via API")

    def update_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Salesforce does not support catalog modification via API")

    def upsert_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        """SF as target: stamp target_* fields on the passed Catalog."""
        try:
            for entity in catalog.entities:
                pass
            return PluginResponse.success(data=catalog)
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_catalog(self, catalog: Catalog | str, **kwargs: Any) -> PluginResponse[Catalog]:
        return PluginResponse.not_implemented("Salesforce does not support catalog deletion via API")

    
    # TABLE
    def get_table(self, catalog: Catalog, entity: Entity, **kwargs: Any) -> PluginResponse[Entity]:
        """Describe a Salesforce object into a Entity (cached on self.catalog).

        If a Entity is passed, its name drives the describe call and its
        fields are populated in-place before returning it.
        """
        try:
            incoming = entity if isinstance(entity, Entity) else None
            name = entity.name if isinstance(entity, Entity) else entity

            if name not in self.catalog.entity_map:
                describe = getattr(self.rest, name).describe()
                fields = []
                for f in describe.get('fields', []):
                    sf_type = f['type']
                    if sf_type in ('address', 'location'):
                        logger.debug(f"Skipping compound field {f['name']} ({sf_type}) on {name}")
                        continue
                    pv = f.get('picklistValues') or []
                    fields.append(Column(
                        name=f['name'],
                        raw_type=sf_type,
                        primary_key=(f['name'] == 'Id'),
                        nullable=f.get('nillable', True),
                        unique=f.get('unique', False),
                        max_length=f.get('length') or None,
                        precision=f.get('precision') or None,
                        scale=f.get('scale') or None,
                        read_only=not f.get('updateable', True),
                        default_value=f.get('defaultValue'),
                        enum_values=[v['value'] for v in pv if v.get('active')] or None,
                    ))
                t = Entity(name=name, fields=fields)
                self.catalog.entities.append(t)
                t._catalog = self.catalog

            result = self.catalog.entity_map[name]
            if incoming is not None:
                incoming.fields = result.fields
                return PluginResponse.success(data=incoming)
            return PluginResponse.success(data=result)
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_entity(self, catalog: Catalog, entity: Entity, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Salesforce does not support entity creation via data API")

    def update_entity(self, catalog: Catalog, entity: Entity, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Salesforce does not support entity modification via data API")

    def upsert_entity(self, catalog: Catalog, entity: Entity, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Salesforce does not support entity creation via data API")

    def delete_entity(self, catalog: Catalog, entity: Entity, **kwargs: Any) -> PluginResponse[Entity]:
        return PluginResponse.not_implemented("Salesforce does not support entity deletion via data API")

    
    # COLUMN
    def get_field(self, catalog: Catalog, entity: Entity, field: Column | str, **kwargs: Any) -> PluginResponse[Column]:
        name = entity.name if isinstance(entity, Entity) else entity
        col_name = field.name if isinstance(field, Column) else field

        table_result = self.get_table(catalog, name)
        if not table_result.ok or not table_result.data:
            return PluginResponse.not_found(f"Entity {name} not found")

        col = table_result.data.field_map.get(col_name)
        if not col:
            return PluginResponse.not_found(f"{name}.{col_name} not found")

        if isinstance(field, Column):
            
            field.raw_type = col.raw_type
            field.primary_key = col.primary_key
            field.nullable = col.nullable
            field.unique = col.unique
            field.max_length = col.max_length
            field.precision = col.precision
            field.scale = col.scale
            field.read_only = col.read_only
            field.default_value = col.default_value
            field.enum_values = col.enum_values
            return PluginResponse.success(data=field)
        return PluginResponse.success(data=col)

    def create_field(self, catalog: Catalog, entity: Entity, field: Column, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Salesforce does not support field creation via data API")

    def update_field(self, catalog: Catalog, entity: Entity, field: Column, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Salesforce does not support field modification via data API")

    def upsert_field(self, catalog: Catalog, entity: Entity, field: Column, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Salesforce does not support field creation via data API")

    def delete_field(self, catalog: Catalog, entity: Entity, field: Column, **kwargs: Any) -> PluginResponse[Column]:
        return PluginResponse.not_implemented("Salesforce does not support field deletion via data API")

# ============================ DATA ============================
    
    # RECORDS
    def get_data(self, catalog: Catalog, entity: Entity | str, **kwargs: Any) -> PluginResponse[ArrowStream]:
        """Bulk2 full extract. The default read path."""
        try:
            name = entity.name if isinstance(entity, Entity) else entity
            table_result = self.get_table(catalog, name)
            if not table_result.ok or not table_result.data:
                return PluginResponse.not_found(f"Entity {name} not found")
            fields = ', '.join(c.name for c in table_result.data.fields)
            return PluginResponse.success(data=self.bulk_query(f"SELECT {fields} FROM {name}", name))
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_data(self, catalog: Catalog, entity: Entity, data: ArrowStream, **kwargs: Any) -> PluginResponse[ArrowStream]:
        name = entity.name if isinstance(entity, Entity) else entity
        try:
            results = getattr(self.bulk2, name).insert(
                records=[prepare_record(r) for r in records]
            )
            return PluginResponse.success(data=iter(results))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_data(self, catalog: Catalog, entity: Entity | str, data: ArrowStream, **kwargs: Any) -> PluginResponse[ArrowStream]:
        name = entity.name if isinstance(entity, Entity) else entity
        try:
            results = getattr(self.bulk2, name).update(
                records=[prepare_record(r) for r in records]
            )
            return PluginResponse.success(data=iter(results))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_data(self, catalog: Catalog, entity: Entity, data: ArrowStream, **kwargs: Any) -> PluginResponse[ArrowStream]:
        name = entity.name if isinstance(entity, Entity) else entity
        external_id = kwargs.get('external_id_field', 'Id')
        try:
            results = getattr(self.bulk2, name).upsert(
                records=[prepare_record(r) for r in records],
                external_id_field=external_id,
            )
            return PluginResponse.success(data=iter(results))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_data(self, catalog: Catalog, entity: Entity, data: ArrowStream, **kwargs: Any) -> PluginResponse[ArrowStream]:
        name = entity.name if isinstance(entity, Entity) else entity
        try:
            results = getattr(self.bulk2, name).delete(
                records=[prepare_record(r) for r in records]
            )
            return PluginResponse.success(data=iter(results))
        except Exception as e:
            return PluginResponse.error(str(e))

# Explicitly enforce duck-typing compliance at module load time
assert isinstance(SfPlugin(), Plugin)