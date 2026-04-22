from fastapi import APIRouter, Body
from server.plugins.PluginModels import Catalog
from server.core.federation import fanout

router = APIRouter(prefix="/api/entity", tags=["Entity Discovery, and DDL"])

@router.post("/", summary="Fetch entity metadata")
def get_entity(catalog: Catalog = Body(...)):
    return fanout(catalog, "get_entity")

@router.put("/insert", summary="Create")
def create_entity(catalog: Catalog = Body(...)):
    return fanout(catalog, "create_entity")

@router.put("/", summary="Upsert")
def upsert_entity(catalog: Catalog = Body(...)):
    return fanout(catalog, "upsert_entity")

@router.patch("/", summary="Update")
def update_entity(catalog: Catalog = Body(...)):
    return fanout(catalog, "update_entity")

@router.delete("/", summary="Delete entitys")
def delete_entity(catalog: Catalog = Body(...)):
    return fanout(catalog, "delete_entity")