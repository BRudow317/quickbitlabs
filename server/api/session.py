from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from server.api.auth import get_current_user
from server.db.db import session_service
from server.models.user import UserBase
from server.plugins.PluginModels import Catalog

router = APIRouter(prefix="/api/session", tags=["Session"])


@router.get("/", operation_id="get_session", response_model=Catalog)
def get_session(current_user: Annotated[UserBase, Depends(get_current_user)]) -> Catalog:
    """Return the full metadata catalog from CATALOG_REGISTRY."""
    return session_service.load_session(username=current_user.username)


@router.get("/systems", operation_id="list_systems")
def list_systems(current_user: Annotated[UserBase, Depends(get_current_user)]) -> list[str]:
    """Return the plugin names for which synced schema rows exist."""
    return session_service.list_systems()
