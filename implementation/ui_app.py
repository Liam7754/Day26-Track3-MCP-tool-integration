from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

try:
    from .db import SQLiteAdapter, ValidationError
    from .init_db import DEFAULT_DB_PATH, create_database
except ImportError:
    from db import SQLiteAdapter, ValidationError
    from init_db import DEFAULT_DB_PATH, create_database


DB_PATH = Path(DEFAULT_DB_PATH)
if not DB_PATH.exists():
    create_database(DB_PATH)

adapter = SQLiteAdapter(DB_PATH)


def parse_json_field(raw: str, fallback: Any) -> Any:
    if not raw.strip():
        return fallback
    return json.loads(raw)


def show_result(result: Any) -> None:
    if isinstance(result, list):
        st.dataframe(pd.DataFrame(result), width="stretch")
    else:
        st.json(result)


def column_names(schema: list[dict[str, Any]]) -> list[str]:
    return [column["name"] for column in schema]


def editable_columns(schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    editable = []
    for column in schema:
        if column["primary_key"]:
            continue
        if column["default"] is not None and column["name"] in {"created_at"}:
            continue
        editable.append(column)
    return editable


def numeric_columns(schema: list[dict[str, Any]]) -> list[str]:
    numeric_tokens = ("INT", "REAL", "FLOA", "DOUB", "NUM")
    return [
        column["name"]
        for column in schema
        if any(token in column["type"].upper() for token in numeric_tokens)
    ]


def coerce_value(column: dict[str, Any], raw_value: str) -> Any:
    column_type = column["type"].upper()
    if "INT" in column_type:
        return int(raw_value)
    if any(token in column_type for token in ("REAL", "FLOA", "DOUB", "NUM")):
        return float(raw_value)
    return raw_value


def coerce_insert_values(schema: list[dict[str, Any]], raw_values: dict[str, str]) -> dict[str, Any]:
    schema_by_name = {column["name"]: column for column in schema}
    values: dict[str, Any] = {}
    for name, raw_value in raw_values.items():
        if raw_value == "":
            continue
        values[name] = coerce_value(schema_by_name[name], raw_value)
    return values


def build_filter(table: str, schema: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    names = column_names(schema)
    mode = st.radio(
        "Filter mode",
        ["None", "Builder", "JSON"],
        horizontal=True,
        key=f"{prefix}-filter-mode-{table}",
    )
    if mode == "None":
        return {}
    if mode == "JSON":
        raw = st.text_area("Filters JSON", value="{}", key=f"{prefix}-filters-json-{table}")
        return parse_json_field(raw, {})

    selected_column = st.selectbox("Filter column", names, key=f"{prefix}-filter-column-{table}")
    operator = st.selectbox(
        "Operator",
        ["eq", "ne", "gt", "gte", "lt", "lte", "like", "in"],
        key=f"{prefix}-filter-operator-{table}",
    )
    raw_value = st.text_input("Filter value", key=f"{prefix}-filter-value-{table}")
    if raw_value == "":
        return {}
    if operator == "in":
        value = [item.strip() for item in raw_value.split(",") if item.strip()]
    else:
        value = raw_value
    return {selected_column: {operator: value}}


st.set_page_config(page_title="SQLite MCP Lab Demo", layout="wide")
st.title("SQLite MCP Lab Demo")

left, right = st.columns([1, 2])

with left:
    st.subheader("Schema")
    if st.button("Reset database"):
        create_database(DB_PATH)
        st.success("Database reset")

    tables = adapter.list_tables()
    selected_table = st.selectbox("Table", tables, key="selected-table")
    schema = adapter.get_table_schema(selected_table)
    st.dataframe(pd.DataFrame(schema), width="stretch")

with right:
    tab_search, tab_insert, tab_aggregate, tab_demo = st.tabs(["Search", "Insert", "Aggregate", "Demo"])
    all_columns = column_names(schema)

    with tab_search:
        selected_columns = st.multiselect(
            "Columns",
            all_columns,
            default=all_columns,
            key=f"search-columns-{selected_table}",
        )
        filters = build_filter(selected_table, schema, "search")
        order_by = st.selectbox("Order by", [""] + all_columns, key=f"search-order-by-{selected_table}")
        descending = st.checkbox("Descending", value=True, key=f"search-desc-{selected_table}")
        limit = st.number_input("Limit", min_value=1, max_value=100, value=20, key=f"search-limit-{selected_table}")
        offset = st.number_input("Offset", min_value=0, value=0, key=f"search-offset-{selected_table}")

        if st.button("Run search", key=f"run-search-{selected_table}"):
            try:
                result = adapter.search(
                    selected_table,
                    columns=selected_columns,
                    filters=filters,
                    limit=int(limit),
                    offset=int(offset),
                    order_by=order_by or None,
                    descending=descending,
                )
                show_result(result)
            except (ValidationError, ValueError, json.JSONDecodeError) as exc:
                st.error(str(exc))

    with tab_insert:
        raw_values: dict[str, str] = {}
        for column in editable_columns(schema):
            raw_values[column["name"]] = st.text_input(
                f"{column['name']} ({column['type']})",
                key=f"insert-{selected_table}-{column['name']}",
            )

        if st.button("Insert row", key=f"insert-row-{selected_table}"):
            try:
                values = coerce_insert_values(schema, raw_values)
                show_result(adapter.insert(selected_table, values))
            except (ValidationError, ValueError) as exc:
                st.error(str(exc))

    with tab_aggregate:
        metric = st.selectbox("Metric", ["count", "avg", "sum", "min", "max"], key=f"aggregate-metric-{selected_table}")
        metric_columns = numeric_columns(schema) if metric in {"avg", "sum"} else all_columns
        column = st.selectbox("Column", [""] + metric_columns, key=f"aggregate-column-{selected_table}-{metric}")
        group_by = st.selectbox("Group by", [""] + all_columns, key=f"aggregate-group-by-{selected_table}")
        aggregate_filters = build_filter(selected_table, schema, "aggregate")

        if st.button("Run aggregate", key=f"run-aggregate-{selected_table}"):
            try:
                result = adapter.aggregate(
                    selected_table,
                    metric,
                    column=column or None,
                    filters=aggregate_filters,
                    group_by=group_by or None,
                )
                show_result(result)
            except (ValidationError, ValueError, json.JSONDecodeError) as exc:
                st.error(str(exc))

    with tab_demo:
        c1, c2, c3, c4 = st.columns(4)
        if c1.button("Cohort A1"):
            show_result(adapter.search("students", filters={"cohort": "A1"}, order_by="score", descending=True))
        if c2.button("Insert sample"):
            try:
                suffix = datetime.now().strftime("%Y%m%d%H%M%S%f")
                show_result(
                    adapter.insert(
                        "students",
                        {
                            "name": "UI Demo Student",
                            "cohort": "A1",
                            "score": 92.0,
                            "email": f"ui.demo.{suffix}@example.edu",
                        },
                    )
                )
            except ValidationError as exc:
                st.error(str(exc))
        if c3.button("Avg by cohort"):
            show_result(adapter.aggregate("students", "avg", column="score", group_by="cohort"))
        if c4.button("Invalid table"):
            try:
                adapter.search("missing_table")
            except ValidationError as exc:
                st.error(str(exc))
