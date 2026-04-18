from __future__ import annotations
from typing import TYPE_CHECKING, Any, TypedDict, cast
from server.plugins.PluginProtocol import Plugin
from server.plugins.PluginRegistry import get_plugin, PLUGIN
from server.plugins.PluginModels import Catalog, Entity, OperatorGroup

class PluginPlan(TypedDict):
    plugin: Plugin
    catalog: Catalog

class FederationPlan(TypedDict):
    plans: dict[str, PluginPlan]           # per-system pushdown
    cross_system_ops: list[OperatorGroup]  # post-join filters for DuckDB
    output_catalog: Catalog                # master catalog = final result shape

def _collect_plugins_from_group(group: OperatorGroup) -> set[str]:
    """Recursively collect all plugin names referenced in an OperatorGroup tree."""
    plugins: set[str] = set()
    for op in group.operation_group:
        if isinstance(op, OperatorGroup):
            plugins |= _collect_plugins_from_group(op)
        elif op.independent.locator and op.independent.locator.plugin:
            plugins.add(op.independent.locator.plugin)
    return plugins

def resolve_catalog_plugins(master_catalog: Catalog) -> FederationPlan:
    """
    Slices a federated master Catalog into system-specific sub-catalogs
    and instances the required plugin for each.
    Cross-system operator groups are preserved for the DuckDB federation
    layer to apply post-join.
    """
    if not master_catalog.entities:
        raise ValueError("Catalog must contain at least one entity to route.")

    # 1. Group entities by their target system
    system_entity_map: dict[str, list[Entity]] = {}
    for entity in master_catalog.entities:
        locator = entity.locator
        if not locator or not locator.plugin:
            raise ValueError(f"Entity '{entity.name}' is missing a plugin locator.")
        system_entity_map.setdefault(locator.plugin, []).append(entity)

    # 2. Classify operator groups once, before building sub-catalogs
    single_system_ops: dict[str, list[OperatorGroup]] = {}
    cross_system_ops: list[OperatorGroup] = []

    for group in master_catalog.operator_groups:
        systems = _collect_plugins_from_group(group)
        if len(systems) == 1:
            single_system_ops.setdefault(systems.pop(), []).append(group)
        else:
            cross_system_ops.append(group)

    # 3. Build execution plan
    plans: dict[str, PluginPlan] = {}
    for system_name, entities in system_entity_map.items():
        plugin_instance = get_plugin(cast(PLUGIN, system_name))

        valid_sorts = [
            s for s in master_catalog.sort_columns
            if s.column.locator and s.column.locator.plugin == system_name
        ]

        sub_catalog = master_catalog.model_copy(update={
            "entities": entities,
            "joins": [],
            "sort_columns": valid_sorts,
            "operator_groups": single_system_ops.get(system_name, []),
            "limit": None,
        })

        plans[system_name] = {
            "plugin": plugin_instance,
            "catalog": sub_catalog,
        }

    return {
        "plans": plans,
        "cross_system_ops": cross_system_ops,
        "output_catalog": master_catalog,
    }