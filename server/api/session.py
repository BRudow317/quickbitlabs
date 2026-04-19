from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from server.api.auth import get_current_user
from server.models.user import UserBase
from server.plugins.PluginModels import Catalog
from server.services.new_session import load_session, METADATA_DIR

router = APIRouter(prefix="/api/session", tags=["Session"])


@router.get("/", operation_id="get_session", response_model=Catalog)
def get_session(current_user: Annotated[UserBase, Depends(get_current_user)]) -> Catalog:
    """Return the full metadata catalog from the parquet cache."""
    return load_session()


@router.get("/systems", operation_id="list_systems")
def list_systems(current_user: Annotated[UserBase, Depends(get_current_user)]) -> list[str]:
    """Return the system names for which a metadata cache exists."""
    if not METADATA_DIR.exists():
        return []
    return sorted(p.stem for p in METADATA_DIR.glob("*.parquet"))
