"""python "Q:/scripts/boot.py" -l ./.logs -config"""
from __future__ import annotations
import logging
logger: logging.Logger = logging.getLogger(__name__)

from pathlib import Path
from typing import Literal
import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.routing import APIRoute
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from configs.settings import settings
from api import auth, users
from api import (
    catalog,
    entity,
    column,
    data,
    info,
    session,
    migration,
    registry,
    files,
)

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

def _generate_unique_id(route: APIRoute) -> str:
    return route.name

class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.time()
        logger.info(f"→ {request.method} {request.url.path}")
        response = await call_next(request)
        elapsed = time.time() - start
        logger.info(f"← {request.method} {request.url.path} | {response.status_code} | {elapsed:.3f}s")
        return response

def create_app() -> FastAPI:
    _app = FastAPI(
        title=settings.project_name,
        generate_unique_id_callback=_generate_unique_id,
        lifespan=lifespan,
    )

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=settings.allow_credentials,
        allow_methods=settings.allow_methods,
        allow_headers=settings.allow_headers,
    )

    _app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
    _app.include_router(users.router, prefix="/api/users", tags=["users"])

    _app.include_router(catalog.router)
    _app.include_router(entity.router)
    _app.include_router(column.router)
    _app.include_router(data.router)

    _app.include_router(session.router)
    _app.include_router(registry.router, prefix="/api/registry", tags=["registry"])
    _app.include_router(files.router, prefix="/api/files", tags=["files"])
    _app.include_router(migration.router, prefix="/api/migration", tags=["migration"])

    frontend = Path(settings.frontend)
    if frontend.exists():
        from fastapi.responses import FileResponse, JSONResponse

        @_app.get("/{full_path:path}")
        async def spa_fallback(full_path: str):
            # 1. API 404s should stay as JSON 404s
            if full_path.startswith("api/"):
                return JSONResponse(status_code=404, content={"detail": "Not Found"})

            # 2. Check if the path points to a real file (assets, favicons, etc.)
            file_path = frontend / full_path
            if full_path and file_path.is_file():
                return FileResponse(str(file_path))

            # 3. Everything else (SPA routes) serves index.html
            index_path = frontend / "index.html"
            if index_path.exists():
                return FileResponse(str(index_path))
            
            return JSONResponse(status_code=404, content={"detail": "Frontend not built"})

    return _app

app = create_app()

def start_app(mode: Literal["development", "staging", "production"]) -> None:
    import uvicorn
    reload = True if mode == "development" else False
    uvicorn.run("server.app:app", host="0.0.0.0", port=8000, reload=reload)
