from __future__ import annotations

import pyarrow as pa
import pytest
from pydantic import ValidationError

from server.plugins.PluginModels import Column, Operator, OperatorGroup


@pytest.fixture
def sample_column() -> Column:
    return Column(
        name="STATUS",
        qualified_name="ORDERS.STATUS",
        raw_type="VARCHAR2",
        arrow_type_id="string",
        properties={"python_type": "string"},
    )


def test_operator_accepts_all_supported_literals(sample_column: Column) -> None:
    allowed = [
        "=",
        "==",
        "!=",
        ">",
        "<",
        ">=",
        "<=",
        "IN",
        "LIKE",
        "IS NULL",
        "IS NOT NULL",
    ]

    for op in allowed:
        model = Operator(independent=sample_column, operator=op, dependent="active")
        assert model.operator == op


def test_operator_rejects_unsupported_literal(sample_column: Column) -> None:
    with pytest.raises(ValidationError):
        Operator(independent=sample_column, operator="BETWEEN", dependent="a,b")


def test_operator_dependent_accepts_string_column_field_or_none(sample_column: Column) -> None:
    rhs_column = Column(
        name="UPDATED_AT",
        qualified_name="ORDERS.UPDATED_AT",
        raw_type="TIMESTAMP",
        arrow_type_id="timestamp_ns",
        properties={"python_type": "datetime"},
    )
    rhs_field = pa.field("UPDATED_AT", pa.timestamp("ns"))

    a = Operator(independent=sample_column, operator="=", dependent="active")
    b = Operator(independent=sample_column, operator="=", dependent=rhs_column)
    c = Operator(independent=sample_column, operator="=", dependent=rhs_field)
    d = Operator(independent=sample_column, operator="IS NULL", dependent=None)

    assert isinstance(a.dependent, str)
    assert isinstance(b.dependent, Column)
    assert isinstance(c.dependent, pa.Field)
    assert d.dependent is None


def test_operator_group_defaults_to_empty_operator_list() -> None:
    group = OperatorGroup(condition="AND")
    assert group.operators == []


def test_operator_group_supports_nested_groups_and_operators(sample_column: Column) -> None:
    left = Operator(independent=sample_column, operator="=", dependent="active")
    right = Operator(independent=sample_column, operator="LIKE", dependent="pending%")
    nested = OperatorGroup(condition="OR", operators=[left, right])
    root = OperatorGroup(condition="AND", operators=[nested])

    assert root.condition == "AND"
    assert len(root.operators) == 1
    assert isinstance(root.operators[0], OperatorGroup)
    assert len(root.operators[0].operators) == 2


def test_operator_group_rejects_invalid_condition() -> None:
    with pytest.raises(ValidationError):
        OperatorGroup(condition="XOR", operators=[])
