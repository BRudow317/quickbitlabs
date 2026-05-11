from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from server.db.db import server_db
from server.configs.settings import settings

router = APIRouter()

_REQUIRED_TABLES = {"QBL_USERS", "CATALOG_REGISTRY", "USER_SESSION", "USER_REFRESH_TOKEN", "USER_SIGN_IN"}
_PLACEHOLDER = "CHANGE_ME"


@router.get("/api/health", tags=["health"], operation_id="health_check")
def health_check() -> JSONResponse:
    """
    Liveness / readiness probe.

    Returns 200 when all subsystems are healthy, 503 when any check fails.
    Safe to call without authentication — expose to load balancers and Docker HEALTHCHECK.
    """
    checks: dict = {}
    overall = "ok"

    # --- Database connectivity -------------------------------------------
    try:
        server_db.ping()
        checks["db"] = {"status": "ok", "service": server_db._oracle_service}
    except Exception as exc:
        checks["db"] = {"status": "error", "detail": str(exc)}
        overall = "degraded"

    # --- Schema / DDL completeness ----------------------------------------
    if checks["db"]["status"] == "ok":
        try:
            placeholders = ",".join(f"'{t}'" for t in _REQUIRED_TABLES)
            sql = f"SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME IN ({placeholders})"
            with server_db.connect().cursor() as cur:
                cur.execute(sql)
                found = {row[0] for row in cur.fetchall()}
            missing = sorted(_REQUIRED_TABLES - found)
            checks["schema"] = {
                "status":   "ok" if not missing else "degraded",
                "found":    sorted(found),
                "missing":  missing,
            }
            if missing:
                overall = "degraded"
        except Exception as exc:
            checks["schema"] = {"status": "error", "detail": str(exc)}
            overall = "degraded"

    # --- User roles seeded ------------------------------------------------
    if checks["db"]["status"] == "ok":
        try:
            with server_db.connect().cursor() as cur:
                cur.execute("SELECT role_id FROM user_roles ORDER BY role_id")
                roles = [row[0] for row in cur.fetchall()]
            checks["seed"] = {
                "status": "ok" if {"user", "admin"} <= set(roles) else "degraded",
                "roles":  roles,
            }
            if checks["seed"]["status"] != "ok":
                overall = "degraded"
        except Exception as exc:
            checks["seed"] = {"status": "error", "detail": str(exc)}
            overall = "degraded"

    # --- JWT configuration ------------------------------------------------
    try:
        secret = settings.jwt_secret.get_secret_value()
        ok = bool(secret) and secret != _PLACEHOLDER
        checks["jwt"] = {"status": "ok" if ok else "misconfigured"}
        if not ok:
            overall = "degraded"
    except Exception as exc:
        checks["jwt"] = {"status": "error", "detail": str(exc)}
        overall = "degraded"

    status_code = 200 if overall == "ok" else 503
    return JSONResponse({"status": overall, "checks": checks}, status_code=status_code)
