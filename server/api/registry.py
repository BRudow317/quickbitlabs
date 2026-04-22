from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status

from server.api.auth import get_current_user
from server.configs.catalog_registry import CatalogRegistryService
from server.configs.db import oracle_client
from server.models.user import UserBase
from server.plugins.PluginModels import Catalog

router = APIRouter()


def _registry() -> CatalogRegistryService:
    return CatalogRegistryService(oracle_client)


@router.get("/", response_model=list[dict[str, Any]], operation_id="list_registry")
def list_registry(
    current_user: Annotated[UserBase, Depends(get_current_user)],
    registry: CatalogRegistryService = Depends(_registry),
):
    """Return lightweight metadata for all catalogs saved by the current user."""
    return registry.list_entries(current_user.username)


@router.get("/{registry_key}", response_model=Catalog, operation_id="get_registry_entry")
def get_registry_entry(
    registry_key: str,
    current_user: Annotated[UserBase, Depends(get_current_user)],
    registry: CatalogRegistryService = Depends(_registry),
):
    """Retrieve a saved Catalog by key."""
    catalog = registry.get(current_user.username, registry_key)
    if catalog is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"'{registry_key}' not found")
    return catalog


@router.put("/{registry_key}", status_code=status.HTTP_204_NO_CONTENT, operation_id="save_registry_entry")
def save_registry_entry(
    registry_key: str,
    catalog: Catalog,
    current_user: Annotated[UserBase, Depends(get_current_user)],
    registry: CatalogRegistryService = Depends(_registry),
):
    """Save (create or overwrite) a named Catalog for the current user."""
    registry.save(current_user.username, registry_key, catalog)


@router.delete("/{registry_key}", status_code=status.HTTP_204_NO_CONTENT, operation_id="delete_registry_entry")
def delete_registry_entry(
    registry_key: str,
    current_user: Annotated[UserBase, Depends(get_current_user)],
    registry: CatalogRegistryService = Depends(_registry),
):
    """Delete a saved Catalog by key."""
    deleted = registry.delete(current_user.username, registry_key)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"'{registry_key}' not found")
