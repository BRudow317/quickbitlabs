"""
File upload API — POST /api/files/upload

Accepts a raw file upload (csv, parquet, feather, arrow, xlsx), processes it
through the appropriate plugin, saves each entity as encrypted Parquet, and
persists the resulting Catalog to the user's registry.

Response JSON:
    registry_key  — the key under which the catalog was saved
    catalog       — the full Catalog dict (locators point at saved Parquet files)
    registry      — updated list of all registry entries for the current user

After this call the frontend can load a preview by posting the returned Catalog
(with limit=500) to POST /api/data/.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile, status

from server.api.auth import get_current_user
from server.core.catalog_registry import CatalogRegistryService
from server.configs.settings import settings
from server.models.user import UserBase
from server.services import file_service

logger = logging.getLogger(__name__)
router = APIRouter()


def _registry() -> CatalogRegistryService:
    return CatalogRegistryService()


@router.post(
    "/upload",
    status_code=status.HTTP_201_CREATED,
    operation_id="upload_file",
    summary="Upload a file and save it to the catalog registry",
)
async def upload_file(
    file: UploadFile,
    current_user: Annotated[UserBase, Depends(get_current_user)],
    registry: CatalogRegistryService = Depends(_registry),
    registry_key: str = Form(default=None),
) -> dict[str, Any]:
    """
    Process an uploaded file (csv / parquet / feather / arrow / xlsx), persist
    each entity as encrypted Parquet, and save the resulting Catalog under
    *registry_key* (defaults to the filename stem).

    Returns the saved registry_key, the Catalog, and the full updated registry list.
    """
    filename = file.filename or "upload"
    key = registry_key or Path(filename).stem
    enc_key: str | None = settings.UPLOAD_ENCRYPTION_KEY or None

    try:
        file_bytes = await file.read()
        catalog, _preview = file_service.process_upload(
            file_bytes, filename, current_user.username, enc_key
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    except RuntimeError as exc:
        logger.exception("file_service.process_upload failed")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc))

    registry.save(current_user.username, key, catalog)
    logger.info(f"Files API: saved '{key}' for user '{current_user.username}'")

    return {
        "registry_key": key,
        "catalog": catalog.model_dump(),
        "registry": registry.list_entries(current_user.username),
    }
