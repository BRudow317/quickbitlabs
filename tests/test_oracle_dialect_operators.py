from __future__ import annotations

import pyarrow as pa
import pytest

from server.plugins.PluginModels import Assignment, Catalog, Column, Entity, Locator, Operation, OperatorGroup, Sort
from server.plugins.oracle.OracleDialect import (
    build_delete_dml,
    build_select,
    build_update_dml,
    build_insert_dml,
    build_merge_dml,
)


def _loc(entity_name: str) -> Locator:
    return Locator(plugin="oracle", entity_name=entity_name)


def _col(name: str, arrow_type: str = "string", pk: bool = False, entity: str = "ORDERS") -> Column:
    return Column(
        name=name,
        raw_type="VARCHAR2",
        arrow_type_id=arrow_type,
        primary_key=pk,
        locator=_loc(entity),
    )


def _entity(name: str, cols: list[Column]) -> Entity:
    return Entity(name=name, columns=cols)


# ---------------------------------------------------------------------------
# SELECT / filters
# ---------------------------------------------------------------------------

def test_build_select_no_filters() -> None:
    id_col = _col("ID", arrow_type="int64", pk=True)
    entity = _entity("ORDERS", [id_col])
    catalog = Catalog(entities=[entity])

    sql, binds = build_select(catalog)

    assert '"ID"' in sql
    assert "FROM" in sql
    assert "WHERE" not in sql
    assert binds == {}


def test_build_select_with_filter_equality() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    op_group = OperatorGroup(
        condition="AND",
        operation_group=[Operation(independent=status, operator="=", dependent="active")],
    )
    catalog = Catalog(entities=[entity], filters=[op_group])

    sql, binds = build_select(catalog)

    assert 'WHERE' in sql
    assert '"STATUS" = :STATUS' in sql
    assert binds == {"STATUS": "active"}


def test_build_select_with_filter_in_list() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    op_group = OperatorGroup(
        condition="AND",
        operation_group=[Operation(independent=status, operator="IN", dependent=["active", "pending"])],
    )
    catalog = Catalog(entities=[entity], filters=[op_group])

    sql, binds = build_select(catalog)

    assert "IN" in sql
    assert len(binds) == 2


def test_build_select_in_empty_list_short_circuits() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    op_group = OperatorGroup(
        condition="AND",
        operation_group=[Operation(independent=status, operator="IN", dependent=[])],
    )
    catalog = Catalog(entities=[entity], filters=[op_group])

    sql, _ = build_select(catalog)

    assert "1=0" in sql


def test_build_select_is_null() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    op_group = OperatorGroup(
        condition="AND",
        operation_group=[Operation(independent=status, operator="IS NULL", dependent=None)],
    )
    catalog = Catalog(entities=[entity], filters=[op_group])

    sql, binds = build_select(catalog)

    assert "IS NULL" in sql
    assert binds == {}


def test_build_select_pa_field_dependent_uses_named_placeholder() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    stream_field = pa.field("STATUS", pa.string())
    op_group = OperatorGroup(
        condition="AND",
        operation_group=[Operation(independent=status, operator="=", dependent=stream_field)],
    )
    catalog = Catalog(entities=[entity], filters=[op_group])

    sql, binds = build_select(catalog)

    assert ":STATUS" in sql
    assert binds == {}


def test_build_select_with_sort_and_limit() -> None:
    id_col = _col("ID", arrow_type="int64", pk=True)
    entity = _entity("ORDERS", [id_col])
    catalog = Catalog(
        entities=[entity],
        sort_columns=[Sort(column=id_col, direction="DESC")],
        limit=25,
    )

    sql, _ = build_select(catalog)

    assert "ORDER BY" in sql
    assert "DESC" in sql
    assert "FETCH FIRST 25 ROWS ONLY" in sql


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------

def test_build_update_requires_filter() -> None:
    entity = _entity("ORDERS", [_col("STATUS")])
    catalog = Catalog(entities=[entity])

    with pytest.raises(ValueError, match="catalog.filters"):
        build_update_dml(catalog, entity)


def test_build_update_set_from_assignments() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status, _col("ID", pk=True)])
    catalog = Catalog(
        entities=[entity],
        filters=[OperatorGroup(
            condition="AND",
            operation_group=[Operation(independent=_col("ID", pk=True), operator="=", dependent=1)],
        )],
        assignments=[Assignment(column=status, value="inactive")],
    )

    sql, binds = build_update_dml(catalog, entity)

    assert "UPDATE" in sql
    assert "SET" in sql
    assert "WHERE" in sql
    assert "inactive" in binds.values()


# ---------------------------------------------------------------------------
# INSERT
# ---------------------------------------------------------------------------

def test_build_insert_assignment_driven() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    catalog = Catalog(
        entities=[entity],
        assignments=[Assignment(column=status, value="new")],
    )

    sql, binds = build_insert_dml(catalog, entity)

    assert "INSERT INTO" in sql
    assert "new" in binds.values()


def test_build_insert_stream_fallback() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    catalog = Catalog(entities=[entity])

    sql, binds = build_insert_dml(catalog, entity)

    assert "INSERT INTO" in sql
    assert ":STATUS" in sql
    assert binds == {}


# ---------------------------------------------------------------------------
# MERGE
# ---------------------------------------------------------------------------

def test_build_merge_requires_filters() -> None:
    entity = _entity("ORDERS", [_col("ID", pk=True)])
    catalog = Catalog(entities=[entity])

    with pytest.raises(ValueError, match="catalog.filters"):
        build_merge_dml(catalog, entity)


def test_build_merge_on_clause_from_filters() -> None:
    id_col = _col("ID", pk=True)
    status = _col("STATUS")
    entity = _entity("ORDERS", [id_col, status])
    catalog = Catalog(
        entities=[entity],
        filters=[OperatorGroup(
            condition="AND",
            operation_group=[Operation(independent=id_col, operator="=", dependent=pa.field("ID"))],
        )],
    )

    sql, _ = build_merge_dml(catalog, entity)

    assert "MERGE INTO" in sql
    assert "ON" in sql
    assert "tgt." in sql
    assert "src." in sql


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------

def test_build_delete_with_filter_binds() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    catalog = Catalog(
        entities=[entity],
        filters=[OperatorGroup(
            condition="AND",
            operation_group=[Operation(independent=status, operator="=", dependent="active")],
        )],
    )

    sql, binds = build_delete_dml(catalog, entity)

    assert "DELETE FROM" in sql
    assert "WHERE" in sql
    assert "active" in binds.values()


def test_build_delete_stream_fallback_when_no_filters() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    catalog = Catalog(entities=[entity])

    sql, binds = build_delete_dml(catalog, entity)

    assert "DELETE FROM" in sql
    assert ":STATUS" in sql
    assert binds == {}
