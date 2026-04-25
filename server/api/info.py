from __future__ import annotations

from fastapi import APIRouter
from server.configs.settings import settings

router = APIRouter()

@router.get("/info")
def get_info():
    return {
        "project_name": settings.project_name,
        "version": "0.1.0",
    }