from fastapi import APIRouter, HTTPException, Response, UploadFile, File, Form, Body
from fastapi.responses import StreamingResponse
from typing import Annotated, cast
import pyarrow as pa
from server.plugins.PluginModels import Catalog, ArrowReader
from server.plugins.PluginRegistry import get_plugin, PLUGIN
from server.core.federation import duckdb_orchestrator

router = APIRouter(prefix="/api/data", tags=["Federated Data Execution"])


def _single_child(catalog: Catalog):
    """Guard for write operations - data writes require exactly one target system."""
    children: list[Catalog] = catalog.federate
    if not children:
        raise HTTPException(status_code=400, detail="Could not resolve any plugins from the provided Catalog.")
    if len(children) != 1:
        raise HTTPException(status_code=400, detail="Write operations require a single target system.")
    child = children[0]
    if not child.source_type:
        raise HTTPException(status_code=500, detail="Child catalog is missing source_type.")
    return child, get_plugin(cast(PLUGIN, child.source_type))


# ---------------------------------------
# READ
# ---------------------------------------
@router.post("/", summary="Fetch data via Catalog AST")
def get_data(catalog: Catalog = Body(...)):
    children: list[Catalog] = catalog.federate

    if not children:
        raise HTTPException(status_code=400, detail="Could not resolve any plugins from the provided Catalog.")

    # ---------------------------------------------------------
    # PATH A: Single System (Bypass DuckDB for speed)
    # ---------------------------------------------------------
    if len(children) == 1:
        child = children[0]
        if not child.source_type:
            raise HTTPException(status_code=500, detail="Child catalog is missing source_type.")
        plugin = get_plugin(cast(PLUGIN, child.source_type))
        resp = plugin.get_data(child)
        if not resp.ok:
            raise HTTPException(status_code=500, detail=resp.message)
        return Response(
            content=Catalog.serialize_arrow_stream(resp.data),
            media_type="application/vnd.apache.arrow.stream"
        )

    # ---------------------------------------------------------
    # PATH B: Multi-System (Federated DuckDB Join)
    # ---------------------------------------------------------
    children_streams: list[tuple[Catalog, ArrowReader]] = []
    for child in children:
        if not child.source_type:
            raise HTTPException(status_code=500, detail="Child catalog is missing source_type.")
        plugin = get_plugin(cast(PLUGIN, child.source_type))
        resp = plugin.get_data(child)
        if not resp.ok:
            raise HTTPException(status_code=500, detail=f"Plugin '{child.source_type}' failed: {resp.message}")
        children_streams.append((child, resp.data))

    federated_stream = duckdb_orchestrator(catalog, children_streams)
    return Response(
        content=Catalog.serialize_arrow_stream(federated_stream),
        media_type="application/vnd.apache.arrow.stream"
    )


# ---------------------------------------
# INSERT
# ---------------------------------------
@router.put("/insert", summary="Insert new data")
def create_data(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    child, plugin = _single_child(Catalog.model_validate_json(catalog_json))
    arrow_reader: ArrowReader = Catalog.deserialize_arrow_stream(file)

    resp = plugin.create_data(child, arrow_reader)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
    return Response(
        content=Catalog.serialize_arrow_stream(resp.data),
        media_type="application/vnd.apache.arrow.stream"
    )


# ---------------------------------------
# UPSERT
# ---------------------------------------
@router.put("/", summary="Upsert data")
def upsert_data(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    child, plugin = _single_child(Catalog.model_validate_json(catalog_json))
    arrow_reader: ArrowReader = Catalog.deserialize_arrow_stream(file)

    resp = plugin.upsert_data(child, arrow_reader)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
    return Response(
        content=Catalog.serialize_arrow_stream(resp.data),
        media_type="application/vnd.apache.arrow.stream"
    )


# ---------------------------------------
# UPDATE
# ---------------------------------------
@router.patch("/", summary="Update existing data")
def update_data(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    child, plugin = _single_child(Catalog.model_validate_json(catalog_json))
    arrow_reader: ArrowReader = Catalog.deserialize_arrow_stream(file)

    resp = plugin.update_data(child, arrow_reader)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
    return Response(
        content=Catalog.serialize_arrow_stream(resp.data),
        media_type="application/vnd.apache.arrow.stream"
    )


# ---------------------------------------
# DELETE
# ---------------------------------------
@router.delete("/", summary="Delete data", status_code=204)
def delete_data(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    child, plugin = _single_child(Catalog.model_validate_json(catalog_json))
    arrow_reader: ArrowReader = Catalog.deserialize_arrow_stream(file)

    resp = plugin.delete_data(child, arrow_reader)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
    return Response(status_code=204)