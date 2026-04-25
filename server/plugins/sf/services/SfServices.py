from __future__ import annotations

from collections.abc import Generator
from typing import Any, Literal, TYPE_CHECKING
from urllib.parse import urlparse

import pyarrow as pa

from server.plugins.PluginModels import ArrowReader, Catalog, Column, Entity, Locator, Records
from server.plugins.PluginResponse import PluginResponse
from server.plugins.sf.engines.SfBulk2Engine import Bulk2, DEFAULT_QUERY_PAGE_SIZE
from server.plugins.sf.engines.SfClient import SfClient
from server.plugins.sf.engines.SfRestEngine import SfRest
from server.plugins.sf.models.SfDialect import (
    build_count_soql,
    build_null_check_soql,
    build_soql,
    get_object_from_soql,
)
from server.plugins.sf.models.SfTypeMap import SF_TO_ARROW_LITERAL
from server.plugins.sf.services.SfArrowServices import SfArrowFrame

if TYPE_CHECKING:
    from server.plugins.sf.engines.SfBulk2Engine import Bulk2SObject

import logging
logger = logging.getLogger(__name__)

# REST pages at 2 000 records per call with no job-creation overhead.
# Bulk V2 carries ~5-15 s of job-creation + polling latency, so it only wins on
# genuinely large result sets. 50 000 records ≈ 25 REST pages — well within REST's
# sweet spot. Raise if you observe REST pagination becoming a bottleneck.
BULK_V2_THRESHOLD: int = 50_000

class SfService:
    """Salesforce service layer. Owns all describe -> PluginModels mapping,
    SOQL construction, REST vs Bulk2 routing, and Arrow pipeline assembly.
    Returns PluginResponse[T] on all protocol methods.
    Engines are never imported above this layer.
    """
    client: SfClient
    rest: SfRest
    bulk2: Bulk2
    arrow_frame: SfArrowFrame

    def __init__(self, client: SfClient) -> None:
        self.client = client
        self.rest = SfRest(client)
        self.bulk2 = Bulk2(client)
        self.arrow_frame = SfArrowFrame(self.rest, self.bulk2)
        _host = urlparse(client.base_url).hostname or "salesforce"
        self._namespace: str = _host.split(".")[0]  # e.g. "myorg" or "myorg--dev"

    def _describe_to_column(self, field: dict[str, Any], object_name: str = "", ordinal_position: int | None = None) -> Column | None:
        """Map one SF field describe dict to a Column. Returns None for compound types (address, location) that have no Arrow mapping."""
        sf_type = field.get("type", "anyType")
        arrow_id = SF_TO_ARROW_LITERAL.get(sf_type)
        if arrow_id is None:
            return None
        field_name = field["name"]
        is_id = field_name == "Id"
        is_ref = sf_type == "reference"
        ref_targets: list[str] = field.get("referenceTo", [])
        deprecated_hidden: bool = field.get("deprecatedAndHidden", False)
        default_val = field.get("defaultValue")
        return Column(
            name=field_name,
            description=field.get("inlineHelpText") or None,
            locator=Locator(
                plugin="salesforce",
                namespace=self._namespace,
                entity_name=object_name,
            ) if object_name else None,
            alias=field_name,
            raw_type=sf_type,
            arrow_type_id=arrow_id,
            primary_key=is_id,
            is_unique=is_id or field.get("unique", False) or field.get("externalId", False),
            is_nullable=field.get("nillable", True),
            is_read_only=not field.get("createable", False) and not field.get("updateable", False),
            is_compound_key=False,
            is_foreign_key=is_ref,
            foreign_key_entity=ref_targets[0] if is_ref and ref_targets else None,
            foreign_key_column="Id" if is_ref else None,
            is_foreign_key_enforced=field.get("relationshipOrder") is not None if is_ref else False,
            is_computed=field.get("calculated", False) or field.get("autoNumber", False),
            is_deprecated=deprecated_hidden,
            is_hidden=deprecated_hidden,
            max_length=field.get("length") or None,
            precision=field.get("precision") or None,
            scale=field.get("scale") or None,
            serialized_null_value="null",
            default_value=str(default_val) if default_val is not None else None,
            timezone="UTC" if sf_type == "datetime" else None,
            enum_values=[
                pv["value"]
                for pv in field.get("picklistValues", [])
                if pv.get("active")
            ],
            ordinal_position=ordinal_position,
            properties={
                "label": field.get("label"),
                "createable": field.get("createable", False),
                "updateable": field.get("updateable", False),
                "filterable": field.get("filterable", False),
                "reference_to": ref_targets,
                "relationship_name": field.get("relationshipName"),
            },
        )

    def _describe_to_entity(self, describe: dict[str, Any]) -> Entity:
        """Map a full SObject describe response to an Entity with all mappable columns."""
        object_name = describe.get("name", None)
        if not object_name:
            raise ValueError("Describe response is missing 'name' field.")
        columns = [
            col
            for i, f in enumerate(describe.get("fields", []))
            if (col := self._describe_to_column(f, object_name, ordinal_position=i + 1)) is not None
        ]
        return Entity(
            name=object_name,
            alias=object_name,
            description=describe.get("label"),
            plugin="salesforce",
            namespace=self._namespace,
            entity_type="api_endpoint",
            columns=columns,
        )

    def get_catalog(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Catalog]:
        """
        Salesforce Discovery & Hydration Lifecycle:

        Empty catalog  → full discovery: describe all migratable SObjects, call get_entity for each, replace shallow stubs with fully hydrated Entities.
        Populated catalog → targeted hydration: call get_entity for each requested entity, replace with the fresh describe result. If the input entity contained
        a column projection, only those columns are returned.
        """
        try:
            catalog.name = catalog.name or self._namespace
            catalog.namespace = self._namespace
            catalog.source_type = "salesforce"

            if not catalog.entities:
                sobjects = self.rest.describe_migratable()
                catalog.entities = [
                    Entity(name=obj.get("name", ""), alias=obj.get("name", ""))
                    for obj in sobjects
                ]

            validate_nullables: bool = kwargs.get("validate_nullables", False)
            for idx, entity in enumerate(catalog.entities):
                sub_catalog = Catalog(entities=[entity])
                resp = self.get_entity(sub_catalog, **kwargs)

                if not resp.ok or resp.data is None:
                    logger.warning(f"Failed to hydrate entity '{entity.name}': {resp.message}")
                    continue

                hydrated = resp.data
                # PROJECTION: when the input entity named specific columns, filter to only those.
                if entity.columns:
                    requested = {c.name for c in entity.columns}
                    hydrated = hydrated.model_copy(update={
                        "columns": [c for c in hydrated.columns if c.name in requested]
                    })

                if validate_nullables:
                    hydrated = self._verify_entity_nullables(hydrated)

                catalog.entities[idx] = hydrated

            return PluginResponse.success(catalog)
        except Exception as e:
            logger.exception("get_catalog failed")
            return PluginResponse.error(str(e))

    def get_entity(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Entity]:
        try:
            if not catalog.entities:
                return PluginResponse.not_found("No entities specified in catalog.")
            entity = catalog.entities[0]
            object_name = entity.name or ""
            describe = getattr(self.rest, object_name).describe()
            return PluginResponse.success(self._describe_to_entity(describe))
        except Exception as e:
            logger.exception("get_entity failed")
            return PluginResponse.error(str(e))

    def get_column(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[Column]:
        try:
            if not catalog.entities or not catalog.entities[0].columns:
                return PluginResponse.not_found("No column specified in catalog.")
            entity = catalog.entities[0]
            col = entity.columns[0]
            object_name = entity.name or ""
            describe = getattr(self.rest, object_name).describe()
            field_map = {f["name"]: f for f in describe.get("fields", [])}
            sf_field = field_map.get(col.name)
            if not sf_field:
                return PluginResponse.not_found(f"Field '{col.name}' not found on {object_name}.")
            # Preserve ordinal position from source field index
            field_index = next(
                (i for i, f in enumerate(describe.get("fields", [])) if f["name"] == col.name),
                None,
            )
            result = self._describe_to_column(
                sf_field, object_name,
                ordinal_position=field_index + 1 if field_index is not None else None,
            )
            if result is None:
                return PluginResponse.not_found(
                    f"Field '{col.name}' is a compound type with no Arrow mapping."
                )
            return PluginResponse.success(result)
        except Exception as e:
            logger.exception("get_column failed")
            return PluginResponse.error(str(e))

    def get_record_count(self, object_name: str) -> int:
        """Return the total record count for an SObject via a lightweight REST COUNT() query.
        Falls back to BULK_V2_THRESHOLD + 1 on any error so the caller defaults to Bulk V2.
        """
        try:
            result = self.rest.query(build_count_soql(object_name))
            return result.get("totalSize", 0)
        except Exception:
            logger.warning(f"Could not get record count for '{object_name}'; defaulting to Bulk V2.")
            return BULK_V2_THRESHOLD + 1

    def _verify_entity_nullables(self, entity: Entity) -> Entity:
        """Cross-check columns marked non-nullable by SF describe against live data.
        If null values are found in a supposedly non-nullable column, is_nullable is
        corrected to True so downstream systems can trust the catalog value.
        Only filterable columns are checked (non-filterable columns cannot be used in WHERE).
        """
        object_name = entity.name or ""
        for col in entity.columns:
            if col.is_nullable or not col.properties.get("filterable", True):
                continue
            try:
                result = self.rest.query(build_null_check_soql(object_name, col.name))
                if result.get("totalSize", 0) > 0:
                    col.is_nullable = True
                    logger.warning(
                        f"{object_name}.{col.name}: SF describe reports non-nullable "
                        f"but null values exist in data; correcting catalog to nullable."
                    )
            except Exception:
                logger.warning(f"{object_name}.{col.name}: null check query failed; leaving is_nullable as-is.")
        return entity

    def get_data(self, catalog: Catalog, **kwargs: Any) -> PluginResponse[ArrowReader]:
        try:
            if not catalog.entities: return PluginResponse.error("Catalog must contain at least one entity.")
            include_deleted: bool = kwargs.get("include_deleted", False)
            # Explicit query_type kwarg ('rest' or 'bulk2') bypasses the count check entirely.
            explicit_query_type: str | None = kwargs.get("query_type")
            streams: list[ArrowReader] = []
            for entity in catalog.entities:
                # TODO sniff the total record counts for all entities up front and determine if Arrow Tables in memory is feasible or if encrypted feather is needed.
                # TODO implement custom data frame functionality to manage the Federated query protocols for multiple entities, including joining on the fly and limiting entities and fields to the minimum viable requirements for the query. Current state implements a list of iterators of the objects, and that's a violation of the contract.
                object_name = entity.name or ""
                sub_catalog = catalog.model_copy(update={"entities": [entity]})
                soql: str = kwargs.get("soql", None) or build_soql(sub_catalog, entity)
                # --- Route: base64 fields always force REST (Bulk V2 CSV can't carry blobs)
                has_blob = any(c.raw_type == "base64" for c in entity.columns if c.raw_type)
                if has_blob:
                    use_bulk = False
                    logger.info(f"{object_name}: base64 fields — forcing REST")

                # --- Route: explicit caller override bypasses count check
                elif explicit_query_type is not None:
                    use_bulk = explicit_query_type.lower() == "bulk2"

                # --- Route: count-based decision
                else:
                    count = self.get_record_count(object_name)
                    if count == 0:
                        # Empty object — skip the data call entirely and preserve the schema
                        logger.info(f"{object_name}: 0 records — skipping data call")
                        empty_schema = pa.schema([
                            pa.field(c.name, c.arrow_type or pa.string(), nullable=True)
                            for c in entity.columns
                            if c.arrow_type is not None
                        ])
                        streams.append(pa.RecordBatchReader.from_batches(empty_schema, iter([])))
                        continue
                    use_bulk = count > BULK_V2_THRESHOLD
                    logger.info(
                        f"{object_name}: {count:,} records → {'Bulk V2' if use_bulk else 'REST'}"
                    )

                if use_bulk:
                    streams.append(self.arrow_frame.bulk_to_arrow_stream(
                        object_name, soql, sub_catalog, include_deleted=include_deleted,
                    ))
                else:
                    streams.append(self.arrow_frame.rest_to_arrow_stream(
                        soql, sub_catalog, include_deleted=include_deleted,
                    ))
            if len(streams) == 1: return PluginResponse.success(streams[0])
            first_schema = streams[0].schema
            def _chain():
                for s in streams:
                    yield from s
            return PluginResponse.success(pa.RecordBatchReader.from_batches(first_schema, _chain()))
        except Exception as e:
            logger.exception(f"get_data failed: {e}")
            return PluginResponse.error(str(e))

    def create_data(self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[ArrowReader]:
        try:
            if not catalog.entities: return PluginResponse.error("Catalog must contain at least one entity.")
            table = self.arrow_frame.stream_to_table(data)
            all_results: list[dict] = []
            for entity in catalog.entities:
                object_name = entity.name or ""
                keep = [c.name for c in entity.columns if c.properties.get("createable") and c.name in table.column_names]
                results = getattr(self.bulk2, object_name).insert(
                    table.select(keep),
                    chunk_size=kwargs.get("chunk_size"),
                    wait=kwargs.get("wait", 5),
                )
                all_results.extend(results)
            return PluginResponse.success(self.arrow_frame.results_to_stream(all_results))
        except Exception as e:
            logger.exception(f"create_data failed: {e}")
            return PluginResponse.error(str(e))

    def update_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[ArrowReader]:
        try:
            if not catalog.entities: return PluginResponse.error("Catalog must contain at least one entity.")
            table = self.arrow_frame.stream_to_table(data)
            all_results: list[dict] = []
            for entity in catalog.entities:
                object_name = entity.name or ""
                updateable = {c.name for c in entity.columns if c.properties.get("updateable")} | {"Id"}
                keep = [n for n in table.column_names if n in updateable]
                results = getattr(self.bulk2, object_name).update(
                    table.select(keep),
                    chunk_size=kwargs.get("chunk_size"),
                    wait=kwargs.get("wait", 5),
                )
                all_results.extend(results)
            return PluginResponse.success(self.arrow_frame.results_to_stream(all_results))
        except Exception as e:
            logger.exception(f"update_data failed: {e}")
            return PluginResponse.error(str(e))

    def upsert_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[ArrowReader]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog must contain at least one entity.")
            # Explicit caller kwarg overrides entity-level properties and auto-detection.
            caller_ext_id: str | None = kwargs.get("external_id_field")
            table = self.arrow_frame.stream_to_table(data)
            all_results: list[dict] = []
            for entity in catalog.entities:
                object_name = entity.name or ""
                # Resolution order: caller kwarg → entity.properties → auto-detect from catalog.
                external_id_field = (
                    caller_ext_id
                    or entity.properties.get("external_id_field")
                    or next((c.name for c in entity.columns if c.is_unique and not c.primary_key), None)
                )
                if not external_id_field:
                    logger.error(
                        f"upsert_data: no external ID found for {object_name} — "
                        f"set entity.properties['external_id_field'] or pass the kwarg explicitly."
                    )
                    continue
                updateable = (
                    {c.name for c in entity.columns if c.properties.get("updateable")}
                    | {external_id_field}
                )
                keep = [n for n in table.column_names if n in updateable]
                results = getattr(self.bulk2, object_name).upsert(
                    table.select(keep),
                    external_id_field=external_id_field,
                    chunk_size=kwargs.get("chunk_size"),
                    wait=kwargs.get("wait", 5),
                )
                all_results.extend(results)
            return PluginResponse.success(self.arrow_frame.results_to_stream(all_results))
        except Exception as e:
            logger.exception(f"upsert_data failed: {e}")
            return PluginResponse.error(str(e))

    def delete_data(
        self, catalog: Catalog, data: ArrowReader, **kwargs: Any
    ) -> PluginResponse[None]:
        try:
            if not catalog.entities:
                return PluginResponse.error("Catalog must contain at least one entity.")
            table = self.arrow_frame.stream_to_table(data, keep_cols={"Id"})
            for entity in catalog.entities:
                object_name = entity.name or ""
                getattr(self.bulk2, object_name).delete(
                    table,
                    wait=kwargs.get("wait", 5),
                )
            return PluginResponse.success(None)
        except Exception as e:
            logger.exception(f"delete_data failed: {e}")
            return PluginResponse.error(str(e))

    # Plugin Uniques
    def rest_query(self,
                   soql: str,
                   object_name: str | None = None,
                   return_type: Literal['Records', 'ArrowReader'] = 'Records'
                   ) -> Records | ArrowReader:
        """Execute REST query and return as Records (dicts) or ArrowReader."""
        records_iter = (
            {k: v for k, v in r.items() if k != 'attributes'}
            for r in self.rest.query_all_iter(soql)
        )
        if return_type == 'ArrowReader':
            records = list(records_iter)
            if not records:
                return pa.RecordBatchReader.from_batches(pa.schema([]), iter([]))
            table = pa.Table.from_pylist(records)
            return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        return records_iter

    def bulk_query(
        self,
        soql: str,
        object_name: str | None = None,
        return_type: Literal['Records', 'ArrowReader', 'CSV'] = 'ArrowReader',
        page_size: int = DEFAULT_QUERY_PAGE_SIZE,
    ) -> Generator[dict[str, Any], None, None] | ArrowReader:
        """
        Iterate Bulk2 query results as native Python dicts.
        Memory is bounded by page size
        pages are consumed and discarded one at a time.
        """
        # TODO parse soql to extract object name if not provided, since Bulk2 API requires it for routing but REST does not
        if not object_name: raise ValueError("bulk_query requires object_name.")
        sf_obj: Bulk2SObject = getattr(self.bulk2, object_name)
        for csv_bytes in sf_obj.query(soql):
            if return_type == 'ArrowReader':
                yield sf_obj.csv_bytes_to_arrow(csv_bytes)
            elif return_type == 'Records':
                yield from sf_obj.csv_bytes_to_arrow(csv_bytes).to_pylist()
            else:
                yield csv_bytes

    def query(self,
              soql: str,
              object_name: str | None = None,
              query_type: Literal['Rest', 'Bulk2'] = 'Rest',
              return_type: Literal['Records', 'ArrowReader'] = 'Records') -> PluginResponse[Records | ArrowReader]:
        try:
            if not object_name:
                object_name = get_object_from_soql(soql)
            if query_type.lower() == 'rest':
                return PluginResponse.success(self.rest_query(soql, object_name, return_type))
            elif query_type.lower() == 'bulk2':
                if not object_name: raise ValueError(f"query requires object_name and it could not be parsed from the provided SOQL. \n{soql}")
                return PluginResponse.success(self.bulk_query(soql, object_name, return_type))
            else: return PluginResponse.not_implemented(f"Invalid query_type '{query_type}'. Must be 'Rest' or 'Bulk2'.")
        except Exception as e:
            logger.exception(f"query failed: {e}")
            return PluginResponse.error(str(e))

    def get_limits(self) -> dict[str, Any]:
        """Return current Salesforce API limits as a dict."""
        return self.rest.limits()
