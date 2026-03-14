from __future__ import annotations
import os
from fastapi import FastAPI
import uvicorn
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from configs.settings import settings
from api import auth, users, leads

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

app = create_app()

# Serve the static files from the root dist/ folder
# This is where Vite builds your React app
if os.path.exists("../frontend/dist"):
    app.mount("/", StaticFiles(directory="../frontend/dist", html=True), name="static")
else: 
    raise FileNotFoundError("The frontend/dist directory does not exist. Please build the frontend first.")
if __name__ == "__main__":
    # In development, we use reload. In production, we don't.
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)