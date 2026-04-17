from fastapi import APIRouter, HTTPException, Response, UploadFile, File, Form, Body, Query
from typing import Annotated
from server.engine.plugin_utils import resolve_plugin_from_catalog
from server.plugins.PluginModels import Catalog, Entity
from server.engine.ArrowFrame import ArrowFrame as af



router = APIRouter(prefix="/api/data", tags=["Federated Data Execution"])

# -------------------------------------------------------------------
# READ (Mapping to get_data)
# -------------------------------------------------------------------
@router.get("/", summary="Fetch data via Catalog AST")
def get_data(catalog: Catalog = Body(...)):
    """
    Executes a query.
    Accepts: application/json (Catalog)
    Returns: application/vnd.apache.arrow.stream (Binary PyArrow Stream)
    """
    plugin = resolve_plugin_from_catalog(catalog)
    
    resp = plugin.get_data(catalog)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
        
    arrow_bytes = af.serialize_arrow_stream(resp.data)
    return Response(content=arrow_bytes, media_type="application/vnd.apache.arrow.stream")


# -------------------------------------------------------------------
# CREATE (Mapping to create_data)
# -------------------------------------------------------------------
@router.post("/", summary="Insert new records")
def create_records(
    catalog_json: Annotated[str, Form(description="The Catalog AST as a JSON string")],
    file: Annotated[UploadFile, File(description="The Arrow IPC Stream binary")]
):
    """
    Accepts a multipart form containing the routing instructions (catalog) and the data to insert.
    """
    catalog = Catalog.model_validate_json(catalog_json)
    plugin = resolve_plugin_from_catalog(catalog)
    arrow_stream = af.deserialize_arrow_upload(file)
    
    resp = plugin.create_data(catalog, arrow_stream)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
        
    return Response(
        content=af.serialize_arrow_stream(resp.data), 
        media_type="application/vnd.apache.arrow.stream"
    )


# -------------------------------------------------------------------
# UPDATE (Mapping to update_data / PATCH)
# -------------------------------------------------------------------
@router.patch("/", summary="Update existing records")
def update_records(
    catalog_json: Annotated[str, Form(...)],
    file: Annotated[UploadFile, File(...)]
):
    catalog = Catalog.model_validate_json(catalog_json)
    plugin = resolve_plugin_from_catalog(catalog)
    arrow_stream = af.deserialize_arrow_upload(file)
    
    resp = plugin.update_data(catalog, arrow_stream)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
        
    return Response(
        content=af.serialize_arrow_stream(resp.data), 
        media_type="application/vnd.apache.arrow.stream"
    )


# -------------------------------------------------------------------
# UPSERT (Mapping to upsert_data / PUT)
# -------------------------------------------------------------------
@router.put("/", summary="Upsert records")
def upsert_records(
    catalog_json: Annotated[str, Form(...)],
    file: Annotated[UploadFile, File(...)]
):
    catalog = Catalog.model_validate_json(catalog_json)
    plugin = resolve_plugin_from_catalog(catalog)
    arrow_stream = af.deserialize_arrow_upload(file)
    
    resp = plugin.upsert_data(catalog, arrow_stream)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
        
    return Response(
        content=af.serialize_arrow_stream(resp.data), 
        media_type="application/vnd.apache.arrow.stream"
    )


# -------------------------------------------------------------------
# DELETE (Mapping to delete_data)
# -------------------------------------------------------------------
@router.delete("/", summary="Delete records", status_code=204)
def delete_records(
    catalog_json: Annotated[str, Form(...)],
    file: Annotated[UploadFile, File(...)]
):
    """Deletes records. 204 No Content upon success."""
    catalog = Catalog.model_validate_json(catalog_json)
    plugin = resolve_plugin_from_catalog(catalog)
    arrow_stream = af.deserialize_arrow_upload(file)
    
    resp = plugin.delete_data(catalog, arrow_stream)
    if not resp.ok:
        raise HTTPException(status_code=500, detail=resp.message)
        
    return Response(status_code=204)


