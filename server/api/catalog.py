from fastapi import APIRouter, Body
from server.plugins.PluginModels import Catalog
from server.core.federation import resolve_plans, fanout_plan

router = APIRouter(prefix="/api/catalog", tags=["Catalog Discovery, and DDL"])

@router.post("/", summary="Fetch catalog metadata")
def get_catalog(catalog: Catalog = Body(...)):
    return fanout_plan(resolve_plans(catalog), "get_catalog")

@router.put("/insert", summary="Create")
def create_catalog(catalog: Catalog = Body(...)):
    return fanout_plan(resolve_plans(catalog), "create_catalog")

@router.put("/", summary="Upsert")
def upsert_catalog(catalog: Catalog = Body(...)):
    return fanout_plan(resolve_plans(catalog), "upsert_catalog")

@router.patch("/", summary="Update")
def update_catalog(catalog: Catalog = Body(...)):
    return fanout_plan(resolve_plans(catalog), "update_catalog")

@router.delete("/", summary="Delete catalogs")
def delete_catalog(catalog: Catalog = Body(...)):
    return fanout_plan(resolve_plans(catalog), "delete_catalog")