"""
python ../master/master.py --config Q:/quickbitlabs/.env -l Q:/quickbitlabs/logs -v --exec python Q:/quickbitlabs/server/start_server.py
"""
from __future__ import annotations
import sys
from pathlib import Path
from fastapi import FastAPI
import uvicorn
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

SERVER_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SERVER_DIR.parent
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from configs.settings import settings
from api import auth, users
from api import catalog, entity, column, data
from api import session, migration

from fastapi.routing import APIRoute
def custom_generate_unique_id(route: APIRoute) -> str:
    return route.name

def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.PROJECT_NAME,
        generate_unique_id_callback=custom_generate_unique_id,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Auth & Users
    app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
    app.include_router(users.router, prefix="/api/users", tags=["users"])

    # Plugin metadata federation (prefixes defined inside each router)
    app.include_router(catalog.router)
    app.include_router(entity.router)
    app.include_router(column.router)
    app.include_router(data.router)

    # Session metadata (loaded from parquet cache written by sync_systems.py)
    app.include_router(session.router)

    # Migration management
    app.include_router(migration.router, prefix="/api/migration", tags=["migration"])

    return app

app = create_app()

# Serve the static frontend build
frontend_dist = PROJECT_ROOT / "frontend" / "dist"
if frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="static")
else:
    raise FileNotFoundError("The frontend/dist directory does not exist. Please build the frontend first.")

if __name__ == "__main__":
    uvicorn.run("server.start_server:app", host="0.0.0.0", port=8000, reload=True)
