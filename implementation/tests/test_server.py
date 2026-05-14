from __future__ import annotations

from pathlib import Path

import pytest

from implementation.db import SQLiteAdapter, ValidationError
from implementation.init_db import create_database


@pytest.fixture()
def adapter(tmp_path: Path) -> SQLiteAdapter:
    db_path = create_database(tmp_path / "lab.db")
    return SQLiteAdapter(db_path)


def test_database_init(adapter: SQLiteAdapter) -> None:
    assert set(adapter.list_tables()) == {"students", "courses", "enrollments"}
    assert len(adapter.search("students", limit=100)) == 6


def test_list_tables_and_schema(adapter: SQLiteAdapter) -> None:
    schema = adapter.get_table_schema("students")
    assert [column["name"] for column in schema] == ["id", "name", "cohort", "score", "email", "created_at"]
    assert schema[0]["primary_key"] is True


def test_search_filters_order_and_pagination(adapter: SQLiteAdapter) -> None:
    rows = adapter.search(
        "students",
        columns=["name", "score"],
        filters={"cohort": "A1", "score": {"gte": 80}},
        limit=2,
        offset=0,
        order_by="score",
        descending=True,
    )
    assert [row["name"] for row in rows] == ["Chi Le", "An Nguyen"]
    assert set(rows[0]) == {"name", "score"}


def test_search_in_and_like_filters(adapter: SQLiteAdapter) -> None:
    rows = adapter.search("students", filters={"cohort": {"in": ["A1", "C3"]}, "name": {"like": "%Nguyen%"}})
    assert [row["name"] for row in rows] == ["An Nguyen"]


def test_insert_returns_payload_and_id(adapter: SQLiteAdapter) -> None:
    inserted = adapter.insert(
        "students",
        {"name": "New Student", "cohort": "B2", "score": 90.0, "email": "new.student@example.edu"},
    )
    assert inserted["id"] > 0
    assert inserted["name"] == "New Student"
    assert inserted["created_at"]


def test_aggregate_count_avg_sum_min_max(adapter: SQLiteAdapter) -> None:
    assert adapter.aggregate("students", "count")[0]["value"] == 6
    assert round(adapter.aggregate("students", "avg", column="score")[0]["value"], 2) == 85.58
    assert adapter.aggregate("students", "sum", column="score")[0]["value"] == 513.5
    assert adapter.aggregate("students", "min", column="score")[0]["value"] == 73.5
    assert adapter.aggregate("students", "max", column="score")[0]["value"] == 97.0

    grouped = adapter.aggregate("students", "avg", column="score", group_by="cohort")
    assert [row["cohort"] for row in grouped] == ["A1", "B2", "C3"]


def test_bad_table_rejected(adapter: SQLiteAdapter) -> None:
    with pytest.raises(ValidationError, match="unknown table"):
        adapter.search("missing")


def test_bad_column_rejected(adapter: SQLiteAdapter) -> None:
    with pytest.raises(ValidationError, match="unknown column"):
        adapter.search("students", columns=["password"])


def test_bad_operator_rejected(adapter: SQLiteAdapter) -> None:
    with pytest.raises(ValidationError, match="unsupported filter operator"):
        adapter.search("students", filters={"score": {"between": [80, 90]}})


def test_empty_insert_rejected(adapter: SQLiteAdapter) -> None:
    with pytest.raises(ValidationError, match="non-empty"):
        adapter.insert("students", {})


def test_insert_constraint_error_is_clear(adapter: SQLiteAdapter) -> None:
    with pytest.raises(ValidationError, match="insert violates database constraints"):
        adapter.insert(
            "students",
            {"name": "Duplicate Email", "cohort": "A1", "score": 81.0, "email": "an.nguyen@example.edu"},
        )


def test_bad_aggregate_rejected(adapter: SQLiteAdapter) -> None:
    with pytest.raises(ValidationError, match="unsupported aggregate metric"):
        adapter.aggregate("students", "median", column="score")
    with pytest.raises(ValidationError, match="requires a column"):
        adapter.aggregate("students", "avg")


def test_mcp_tool_wrappers_and_resources() -> None:
    from implementation import mcp_server

    create_database(mcp_server.DATABASE_PATH)

    search_payload = mcp_server.search("students", filters={"cohort": "A1"})
    assert search_payload["ok"] is True
    assert search_payload["rows"]

    invalid_payload = mcp_server.search("missing_table")
    assert invalid_payload["ok"] is False
    assert "unknown table" in invalid_payload["error"]

    assert '"tables"' in mcp_server.database_schema()
    assert '"cohort"' in mcp_server.table_schema("students")
