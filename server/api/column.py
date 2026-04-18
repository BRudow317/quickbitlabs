from fastapi import APIRouter, Body
from server.plugins.PluginModels import Catalog
from server.api.api_util import _resolve_plans, _fanout

router = APIRouter(prefix="/api/column", tags=["Column Discovery, and DDL"])

@router.post("/", summary="Fetch catalog metadata")
def get_column(catalog: Catalog = Body(...)):
    return _fanout(_resolve_plans(catalog), "get_column")

@router.put("/insert", summary="Create")
def create_column(catalog: Catalog = Body(...)):
    return _fanout(_resolve_plans(catalog), "create_column")

@router.put("/", summary="Upsert")
def upsert_column(catalog: Catalog = Body(...)):
    return _fanout(_resolve_plans(catalog), "upsert_column")

@router.patch("/", summary="Update")
def update_column(catalog: Catalog = Body(...)):
    return _fanout(_resolve_plans(catalog), "update_column")

@router.delete("/", summary="Delete columns")
def delete_column(catalog: Catalog = Body(...)):
    return _fanout(_resolve_plans(catalog), "delete_column")