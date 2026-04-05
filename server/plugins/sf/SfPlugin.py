from __future__ import annotations

import csv
import io
from typing import Any

from server.plugins.sf.engines.SfClient import SfClient
from server.plugins.sf.engines.SfRestEngine import SfRest
from server.plugins.sf.engines.Sfbulk2Engine import SfBulk2Handler
from server.plugins.sf.models.SfTypeMap import SF_TYPE_MAP, cast_record, prepare_record
from server.plugins.PluginModels import EntityModel, FieldModel, CatalogModel, Records
from server.plugins.PluginResponse import PluginResponse

import logging
logger = logging.getLogger(__name__)


class SfPlugin:
    """
    Facade Interface for higher level Salesforce operations.
    """
    client: SfClient
    catalog: CatalogModel
    _rest: SfRest | None = None
    _bulk2: SfBulk2Handler | None = None

    def __init__(
        self,
        consumer_key: str | None = None,
        consumer_secret: str | None = None,
        org_url: str | None = None,
        catalog: CatalogModel | None = None,
    ):
        self.client = SfClient.create(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            base_url=org_url,
        )
        self.catalog = catalog if catalog is not None else CatalogModel(source_name='salesforce')

    @property
    def rest(self) -> SfRest:
        if self._rest is None:
            self._rest = SfRest(self.client)
        return self._rest

    @property
    def bulk2(self) -> SfBulk2Handler:
        if self._bulk2 is None:
            self._bulk2 = SfBulk2Handler(self.client)
        return self._bulk2

    # Connector specifics
    def query(self, soql: str, object_name: str | None = None) -> Records:
        """REST SOQL query. Best for small, real-time result sets."""
        field_types: dict[str, str] = {}
        if object_name:
            result = self.get_table(object_name)
            if result.ok and result.data:
                field_types = {c.source_name: c.raw_type for c in result.data.fields if c.raw_type}
        for record in self.rest.query_all_iter(soql):
            clean = {k: v for k, v in record.items() if k != 'attributes'}
            yield cast_record(clean, field_types) if field_types else clean

    def bulk_query(self, soql: str, object_name: str | None = None) -> Records:
        """Bulk2 SOQL query. Best for large exports."""
        field_types: dict[str, str] = {}
        if object_name:
            result = self.get_table(object_name)
            if result.ok and result.data:
                field_types = {c.source_name: c.raw_type for c in result.data.fields if c.raw_type}
        for csv_page in self.bulk2.query(soql):
            for record in csv.DictReader(io.StringIO(csv_page)):
                yield cast_record(record, field_types) if field_types else dict(record)

    def get_limits(self) -> dict[str, Any]:
        return self.rest.limits()

# ============================ METADATA ============================
    
    # CATALOG
    def get_catalog(self, catalog: CatalogModel | str | None = None, **kwargs: Any) -> PluginResponse[CatalogModel]:
        """
        Describe Salesforce objects and return a populated CatalogModel.

        Uses the REST describe API throughout: global describe to discover migratable
        objects, then one individual sobject describe per object for field metadata.

        Full discovery (records=None): global describe → filter migratable → per-object describes.
        Specific records: one individual describe per named object.
        """
        try:
            if isinstance(catalog, CatalogModel):
                records = [t.source_name for t in catalog.entities]
                target = catalog
            else:
                records = kwargs.get('records')
                target = CatalogModel(source_name=catalog or 'salesforce')

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

    def create_catalog(self, catalog: CatalogModel | str, **kwargs: Any) -> PluginResponse[CatalogModel]:
        return PluginResponse.not_implemented("Salesforce does not support catalog creation via API")

    def update_catalog(self, catalog: CatalogModel | str, **kwargs: Any) -> PluginResponse[CatalogModel]:
        return PluginResponse.not_implemented("Salesforce does not support catalog modification via API")

    def upsert_catalog(self, catalog: CatalogModel | str, **kwargs: Any) -> PluginResponse[CatalogModel]:
        """SF as target: stamp target_* fields on the passed CatalogModel."""
        if isinstance(catalog, str):
            return PluginResponse.not_implemented("upsert_catalog requires a CatalogModel object, not a string")
        try:
            catalog.target_name = catalog.target_name or 'salesforce'
            for entity in catalog.entities:
                entity.target_name = entity.target_name or entity.source_name
            return PluginResponse.success(data=catalog)
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_catalog(self, catalog: CatalogModel | str, **kwargs: Any) -> PluginResponse[CatalogModel]:
        return PluginResponse.not_implemented("Salesforce does not support catalog deletion via API")

    
    # TABLE
    def get_table(self, entity: EntityModel | str, **kwargs: Any) -> PluginResponse[EntityModel]:
        """Describe a Salesforce object into a EntityModel (cached on self.catalog).

        If a EntityModel is passed, its source_name drives the describe call and its
        fields are populated in-place before returning it.
        """
        try:
            incoming = entity if isinstance(entity, EntityModel) else None
            name = entity.source_name if isinstance(entity, EntityModel) else entity

            if name not in self.catalog.entity_map:
                describe = getattr(self.rest, name).describe()
                fields = []
                for f in describe.get('fields', []):
                    sf_type = f['type']
                    if sf_type in ('address', 'location'):
                        logger.debug(f"Skipping compound field {f['name']} ({sf_type}) on {name}")
                        continue
                    pv = f.get('picklistValues') or []
                    fields.append(FieldModel(
                        source_name=f['name'],
                        python_type=SF_TYPE_MAP.get(sf_type, 'string'),
                        raw_type=sf_type,
                        primary_key=(f['name'] == 'Id'),
                        nullable=f.get('nillable', True),
                        unique=f.get('unique', False),
                        length=f.get('length') or None,
                        precision=f.get('precision') or None,
                        scale=f.get('scale') or None,
                        source_description=f.get('label'),
                        read_only=not f.get('updateable', True),
                        default_value=f.get('defaultValue'),
                        enum_values=[v['value'] for v in pv if v.get('active')] or None,
                    ))
                t = EntityModel(source_name=name, fields=fields)
                self.catalog.entities.append(t)
                t._catalog = self.catalog

            result = self.catalog.entity_map[name]
            if incoming is not None:
                incoming.fields = result.fields
                return PluginResponse.success(data=incoming)
            return PluginResponse.success(data=result)
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_entity(self, entity: EntityModel | str, **kwargs: Any) -> PluginResponse[EntityModel]:
        return PluginResponse.not_implemented("Salesforce does not support entity creation via data API")

    def update_entity(self, entity: EntityModel | str, **kwargs: Any) -> PluginResponse[EntityModel]:
        return PluginResponse.not_implemented("Salesforce does not support entity modification via data API")

    def upsert_entity(self, entity: EntityModel | str, **kwargs: Any) -> PluginResponse[EntityModel]:
        return PluginResponse.not_implemented("Salesforce does not support entity creation via data API")

    def delete_entity(self, entity: EntityModel | str, **kwargs: Any) -> PluginResponse[EntityModel]:
        return PluginResponse.not_implemented("Salesforce does not support entity deletion via data API")

    
    # COLUMN
    def get_field(self, entity: EntityModel | str, field: FieldModel | str, **kwargs: Any) -> PluginResponse[FieldModel]:
        name = entity.source_name if isinstance(entity, EntityModel) else entity
        col_name = field.source_name if isinstance(field, FieldModel) else field

        table_result = self.get_table(name)
        if not table_result.ok or not table_result.data:
            return PluginResponse.not_found(f"EntityModel {name} not found")

        col = table_result.data.field_map.get(col_name)
        if not col:
            return PluginResponse.not_found(f"{name}.{col_name} not found")

        if isinstance(field, FieldModel):
            field.python_type = col.python_type
            field.raw_type = col.raw_type
            field.primary_key = col.primary_key
            field.nullable = col.nullable
            field.unique = col.unique
            field.length = col.length
            field.precision = col.precision
            field.scale = col.scale
            field.source_description = col.source_description
            field.read_only = col.read_only
            field.default_value = col.default_value
            field.enum_values = col.enum_values
            return PluginResponse.success(data=field)
        return PluginResponse.success(data=col)

    def create_field(self, entity: EntityModel | str, field: FieldModel | str, **kwargs: Any) -> PluginResponse[FieldModel]:
        return PluginResponse.not_implemented("Salesforce does not support field creation via data API")

    def update_field(self, entity: EntityModel | str, field: FieldModel | str, **kwargs: Any) -> PluginResponse[FieldModel]:
        return PluginResponse.not_implemented("Salesforce does not support field modification via data API")

    def upsert_field(self, entity: EntityModel | str, field: FieldModel | str, **kwargs: Any) -> PluginResponse[FieldModel]:
        return PluginResponse.not_implemented("Salesforce does not support field creation via data API")

    def delete_field(self, entity: EntityModel | str, field: FieldModel | str, **kwargs: Any) -> PluginResponse[FieldModel]:
        return PluginResponse.not_implemented("Salesforce does not support field deletion via data API")

# ============================ DATA ============================
    
    # RECORDS
    def get_records(self, entity: EntityModel | str, **kwargs: Any) -> PluginResponse[Records]:
        """Bulk2 full extract. The default read path."""
        try:
            name = entity.source_name if isinstance(entity, EntityModel) else entity
            table_result = self.get_table(entity)
            if not table_result.ok or not table_result.data:
                return PluginResponse.not_found(f"EntityModel {name} not found")
            fields = ', '.join(c.source_name for c in table_result.data.fields)
            return PluginResponse.success(data=self.bulk_query(f"SELECT {fields} FROM {name}", name))
        except Exception as e:
            return PluginResponse.error(str(e))

    def create_records(self, entity: EntityModel | str, records: Records, **kwargs: Any) -> PluginResponse[Records]:
        name = entity.source_name if isinstance(entity, EntityModel) else entity
        try:
            results = getattr(self.bulk2, name).insert(
                records=[prepare_record(r) for r in records]
            )
            return PluginResponse.success(data=iter(results))
        except Exception as e:
            return PluginResponse.error(str(e))

    def update_records(self, entity: EntityModel | str, records: Records, **kwargs: Any) -> PluginResponse[Records]:
        name = entity.source_name if isinstance(entity, EntityModel) else entity
        try:
            results = getattr(self.bulk2, name).update(
                records=[prepare_record(r) for r in records]
            )
            return PluginResponse.success(data=iter(results))
        except Exception as e:
            return PluginResponse.error(str(e))

    def upsert_records(self, entity: EntityModel | str, records: Records, **kwargs: Any) -> PluginResponse[Records]:
        name = entity.source_name if isinstance(entity, EntityModel) else entity
        external_id = kwargs.get('external_id_field', 'Id')
        try:
            results = getattr(self.bulk2, name).upsert(
                records=[prepare_record(r) for r in records],
                external_id_field=external_id,
            )
            return PluginResponse.success(data=iter(results))
        except Exception as e:
            return PluginResponse.error(str(e))

    def delete_records(self, entity: EntityModel | str, records: Records, **kwargs: Any) -> PluginResponse[Records]:
        name = entity.source_name if isinstance(entity, EntityModel) else entity
        try:
            results = getattr(self.bulk2, name).delete(
                records=[prepare_record(r) for r in records]
            )
            return PluginResponse.success(data=iter(results))
        except Exception as e:
            return PluginResponse.error(str(e))

