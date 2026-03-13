from __future__ import annotations
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from server.configs.settings import settings
from server.api import auth, users, leads

from fastapi.routing import APIRoute
def custom_generate_unique_id(route: APIRoute) -> str:
    return route.name

def create_app() -> FastAPI:
    app = FastAPI(title=settings.PROJECT_NAME, 
    generate_unique_id_callback = custom_generate_unique_id)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # API Routes
    app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
    app.include_router(users.router, prefix="/api/users", tags=["users"])
    app.include_router(leads.router, prefix="/api/leads", tags=["leads"])
    return app