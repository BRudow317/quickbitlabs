from __future__ import annotations

import pyarrow as pa

from server.plugins.PluginModels import Catalog, Column, Entity, Operator, OperatorGroup, Sort
from server.plugins.oracle.OracleDialect import build_delete_dml, build_filters, build_select, build_update_dml


def _col(name: str, arrow_type: str = "string", pk: bool = False) -> Column:
    return Column(
        name=name,
        qualified_name=name,
        raw_type="VARCHAR2",
        arrow_type_id=arrow_type,
        primary_key=pk,
        properties={"python_type": "string"},
    )


def _entity(name: str, cols: list[Column]) -> Entity:
    return Entity(name=name, qualified_name=name, columns=cols)


def test_build_filters_equals_and_bind_generation() -> None:
    status = _col("STATUS")
    group = OperatorGroup(
        condition="AND",
        operators=[Operator(independent=status, operator="=", dependent="active")],
    )

    where_sql, binds = build_filters([group])

    assert where_sql == " WHERE (STATUS = :bind_0)"
    assert binds == {"bind_0": "active"}


def test_build_filters_double_equals_is_normalized_to_equals() -> None:
    status = _col("STATUS")
    group = OperatorGroup(
        condition="AND",
        operators=[Operator(independent=status, operator="==", dependent="active")],
    )

    where_sql, binds = build_filters([group])

    assert where_sql == " WHERE (STATUS = :bind_0)"
    assert binds == {"bind_0": "active"}


def test_build_filters_in_creates_multiple_binds_in_order() -> None:
    status = _col("STATUS")
    group = OperatorGroup(
        condition="AND",
        operators=[Operator(independent=status, operator="IN", dependent=["active", "pending"])],
    )

    where_sql, binds = build_filters([group])

    assert where_sql == " WHERE (STATUS IN (:bind_0, :bind_1))"
    assert binds == {"bind_0": "active", "bind_1": "pending"}


def test_build_filters_in_empty_list_short_circuits_false_predicate() -> None:
    status = _col("STATUS")
    group = OperatorGroup(
        condition="AND",
        operators=[Operator(independent=status, operator="IN", dependent=[])],
    )

    where_sql, binds = build_filters([group])

    assert where_sql == " WHERE (1=0)"
    assert binds == {}


def test_build_filters_field_dependent_uses_named_bind_placeholder() -> None:
    status = _col("STATUS")
    stream_field = pa.field("status", pa.string())
    group = OperatorGroup(
        condition="AND",
        operators=[Operator(independent=status, operator="=", dependent=stream_field)],
    )

    where_sql, binds = build_filters([group])

    assert where_sql == " WHERE (STATUS = :status)"
    assert binds == {}


def test_build_filters_null_operators_emit_no_binds() -> None:
    status = _col("STATUS")
    group = OperatorGroup(
        condition="AND",
        operators=[
            Operator(independent=status, operator="IS NULL", dependent=None),
            Operator(independent=status, operator="IS NOT NULL", dependent=None),
        ],
    )

    where_sql, binds = build_filters([group])

    assert where_sql == " WHERE (STATUS IS NULL AND STATUS IS NOT NULL)"
    assert binds == {}


def test_build_select_includes_where_sort_and_limit() -> None:
    id_col = _col("ID", arrow_type="int64", pk=True)
    status_col = _col("STATUS")
    entity = _entity("ORDERS", [id_col, status_col])
    op_group = OperatorGroup(
        condition="AND",
        operators=[Operator(independent=status_col, operator="LIKE", dependent="act%")],
    )

    catalog = Catalog(
        entities=[entity],
        operator_groups=[op_group],
        sort_fields=[Sort(entity=entity, column=id_col, direction="DESC")],
        limit=25,
    )

    sql, binds = build_select(catalog)

    assert sql == (
        "SELECT ID, STATUS FROM ORDERS"
        " WHERE (STATUS LIKE :bind_0)"
        " ORDER BY ID DESC"
        " FETCH FIRST 25 ROWS ONLY"
    )
    assert binds == {"bind_0": "act%"}


def test_build_update_requires_where_clause() -> None:
    entity = _entity("ORDERS", [_col("STATUS")])
    catalog = Catalog(entities=[entity], operator_groups=[])

    try:
        build_update_dml(catalog, entity)
    except ValueError as exc:
        assert "No Operator conditions defined" in str(exc)
    else:
        raise AssertionError("Expected ValueError when UPDATE has no WHERE clause")


def test_build_delete_uses_filter_binds() -> None:
    status = _col("STATUS")
    entity = _entity("ORDERS", [status])
    catalog = Catalog(
        entities=[entity],
        operator_groups=[
            OperatorGroup(
                condition="AND",
                operators=[Operator(independent=status, operator="=", dependent="active")],
            )
        ],
    )

    sql, binds = build_delete_dml(catalog, entity)

    assert sql == "DELETE FROM ORDERS WHERE (STATUS = :bind_0)"
    assert binds == {"bind_0": "active"}
