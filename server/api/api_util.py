from __future__ import annotations
from typing import Any
from fastapi import HTTPException
from fastapi.responses import JSONResponse

from server.engine.federation import FederationPlan, PluginPlan, resolve_catalog_plugins
from server.plugins.PluginModels import Catalog

def _resolve_plans(catalog: Catalog) -> dict[str, PluginPlan]:
    federation: FederationPlan = resolve_catalog_plugins(catalog)
    plans: dict[str, PluginPlan] = federation["plans"]
    if not plans:
        raise HTTPException(status_code=400, detail="Could not resolve any plugins from the provided Catalog.")
    return plans


def _fanout(plans: dict[str, PluginPlan], method: str) -> JSONResponse:
    succeeded: dict[str, Any] = {}
    failed: dict[str, str] = {}

    for system_name, plan in plans.items():
        resp = getattr(plan["plugin"], method)(plan["catalog"])
        if resp.ok:
            succeeded[system_name] = resp.data.model_dump(mode="json") if resp.data else None
        else:
            failed[system_name] = resp.message

    status_code = 200 if not failed else (207 if succeeded else 500)
    return JSONResponse(
        status_code=status_code,
        content={"succeeded": succeeded, "failed": failed}
    )