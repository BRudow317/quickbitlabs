from fastapi import APIRouter, HTTPException, Response, UploadFile, File, Form, Body, Query
from typing import Annotated, cast, TYPE_CHECKING
import pyarrow as pa
from server.engine.federation import resolve_catalog_plugins, FederationPlan
from server.plugins.PluginModels import Catalog, Entity
# from server.engine.ArrowFrame import ArrowFrame as af

if TYPE_CHECKING:
    from server.plugins.PluginRegistry import PLUGIN
    from server.engine.federation import PluginPlan
    from server.plugins.PluginModels import ArrowReader

router = APIRouter(prefix="/api/data", tags=["Federated Data Execution"])

# ---------------------------------------
# READ (Mapping to get_data)
# ---------------------------------------
@router.post("/", summary="Fetch data via Catalog AST")
def get_data(catalog: Catalog = Body(...)):
    """
    Retrieves a data request from a catalog.
    Accepts: application/json (Catalog)
    Returns: application/vnd.apache.arrow.stream (Binary PyArrow Stream)
    """
    federation: FederationPlan = resolve_catalog_plugins(catalog)
    plans: dict[str, PluginPlan] = federation["plans"]

    if not plans:
        raise HTTPException(status_code=400, detail="Could not resolve any plugins from the provided Catalog.")

    # ---------------------------------------------------------
    # PATH A: Single System (Bypass DuckDB for speed)
    # ---------------------------------------------------------
    if len(plans) == 1:
        plan: PluginPlan = next(iter(plans.values()))
        resp = plan["plugin"].get_data(plan["catalog"])
        if not resp.ok:
            raise HTTPException(status_code=500, detail=resp.message)

        arrow_bytes = Catalog.serialize_arrow_stream(resp.data)
        return Response(content=arrow_bytes, media_type="application/vnd.apache.arrow.stream")

    # ---------------------------------------------------------
    # PATH B: Multi-System (Federated DuckDB Join)
    # ---------------------------------------------------------
    streams: dict[str, pa.RecordBatchReader] = {}
    for system_name, plan in plans.items():
        resp = plan["plugin"].get_data(plan["catalog"])
        if not resp.ok:
            raise HTTPException(status_code=500, detail=f"Plugin '{system_name}' failed: {resp.message}")
        streams[system_name] = resp.data

    # federated_stream = duckdb_orchestrator(federation, streams)
    # arrow_bytes = Catalog.serialize_arrow_stream(federated_stream)
    # return Response(content=arrow_bytes, media_type="application/vnd.apache.arrow.stream")
    raise HTTPException(status_code=501, detail="Federated joins are not yet implemented.")

@router.put("/insert", summary="Insert new data")
def create_data(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    """
    Accepts a multipart form containing the routing instructions (catalog) and the data to insert.
    Catalog defines intent and routing. File contains the Arrow IPC stream to write.
    """
    catalog = Catalog.model_validate_json(catalog_json)
    federation: FederationPlan = resolve_catalog_plugins(catalog)
    plans: dict[str, PluginPlan] = federation["plans"]

    if not plans:
        raise HTTPException(status_code=400, detail="Could not resolve any plugins from the provided Catalog.")
    if len(plans) != 1:
        raise HTTPException(status_code=400, detail="Write operations require a single target system.")

    plan: PluginPlan = next(iter(plans.values()))
    arrow_reader: ArrowReader = Catalog.deserialize_arrow_stream(file)

    resp = plan["plugin"].create_data(plan["catalog"], arrow_reader)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)

    return Response(
        content=Catalog.serialize_arrow_stream(resp.data),
        media_type="application/vnd.apache.arrow.stream"
    )
def _resolve_single_system_plan(catalog: Catalog) -> PluginPlan:
    """Shared guard for write operations that require exactly one target system."""
    federation: FederationPlan = resolve_catalog_plugins(catalog)
    plans: dict[str, PluginPlan] = federation["plans"]

    if not plans:
        raise HTTPException(status_code=400, detail="Could not resolve any plugins from the provided Catalog.")
    if len(plans) != 1:
        raise HTTPException(status_code=400, detail="Write operations require a single target system.")

    return next(iter(plans.values()))


# ---------------------------------------
# UPDATE (Mapping to update_data / PATCH)
# ---------------------------------------
@router.patch("/", summary="Update existing data")
def update_data(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    catalog = Catalog.model_validate_json(catalog_json)
    plan: PluginPlan = _resolve_single_system_plan(catalog)
    arrow_reader: ArrowReader = Catalog.deserialize_arrow_stream(file)

    resp = plan["plugin"].update_data(plan["catalog"], arrow_reader)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)

    return Response(
        content=Catalog.serialize_arrow_stream(resp.data),
        media_type="application/vnd.apache.arrow.stream"
    )


# ---------------------------------------
# UPSERT (Mapping to upsert_data)
# TODO: resolve route conflict with update_data - consider /upsert path or separate router
# ---------------------------------------
@router.put("/", summary="Upsert data")
def upsert_data(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    catalog = Catalog.model_validate_json(catalog_json)
    plan: PluginPlan = _resolve_single_system_plan(catalog)
    arrow_reader: ArrowReader = Catalog.deserialize_arrow_stream(file)

    resp = plan["plugin"].upsert_data(plan["catalog"], arrow_reader)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)

    return Response(
        content=Catalog.serialize_arrow_stream(resp.data),
        media_type="application/vnd.apache.arrow.stream"
    )


# ---------------------------------------
# DELETE (Mapping to delete_data)
# ---------------------------------------
@router.delete("/", summary="Delete data", status_code=204)
def delete_data(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    """Deletes data matching the catalog predicates. Returns 204 No Content on success."""
    catalog = Catalog.model_validate_json(catalog_json)
    plan: PluginPlan = _resolve_single_system_plan(catalog)
    arrow_reader: ArrowReader = Catalog.deserialize_arrow_stream(file)

    resp = plan["plugin"].delete_data(plan["catalog"], arrow_reader)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)

    return Response(status_code=204)