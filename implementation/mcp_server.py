from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastmcp import FastMCP

try:
    from .db import SQLiteAdapter, ValidationError
    from .init_db import DEFAULT_DB_PATH, create_database
except ImportError:
    from db import SQLiteAdapter, ValidationError
    from init_db import DEFAULT_DB_PATH, create_database


SERVER_NAME = "SQLite Lab MCP Server"
DATABASE_PATH = Path(os.environ.get("SQLITE_LAB_DB", DEFAULT_DB_PATH)).expanduser().resolve()

if not DATABASE_PATH.exists():
    create_database(DATABASE_PATH)

adapter = SQLiteAdapter(DATABASE_PATH)
mcp = FastMCP(SERVER_NAME)


def _ok(data: Any, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"ok": True, "data": data, "metadata": metadata or {}}


def _error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc), "metadata": {"error_type": type(exc).__name__}}


@mcp.tool(name="search")
def search(
    table: str,
    filters: Any | None = None,
    columns: list[str] | None = None,
    limit: int = 20,
    offset: int = 0,
    order_by: str | None = None,
    descending: bool = False,
) -> dict[str, Any]:
    """Search rows in a validated SQLite table with safe filters, ordering, and pagination."""
    try:
        rows = adapter.search(table, columns, filters, limit, offset, order_by, descending)
        return {
            "ok": True,
            "rows": rows,
            "metadata": {
                "table": table,
                "row_count": len(rows),
                "limit": limit,
                "offset": offset,
            },
        }
    except ValidationError as exc:
        return _error(exc)


@mcp.tool(name="insert")
def insert(table: str, values: dict[str, Any]) -> dict[str, Any]:
    """Insert one row into a validated SQLite table and return the inserted payload."""
    try:
        payload = adapter.insert(table, values)
        return _ok(payload, {"table": table})
    except (ValidationError, ValueError) as exc:
        return _error(exc)


@mcp.tool(name="aggregate")
def aggregate(
    table: str,
    metric: str,
    column: str | None = None,
    filters: Any | None = None,
    group_by: str | None = None,
) -> dict[str, Any]:
    """Run a validated aggregate query: count, avg, sum, min, or max."""
    try:
        rows = adapter.aggregate(table, metric, column, filters, group_by)
        return {
            "ok": True,
            "rows": rows,
            "metadata": {
                "table": table,
                "metric": metric,
                "column": column,
                "group_by": group_by,
                "row_count": len(rows),
            },
        }
    except ValidationError as exc:
        return _error(exc)


@mcp.resource("schema://database")
def database_schema() -> str:
    """Return the full database schema as JSON text."""
    return json.dumps(adapter.database_schema(), indent=2)


@mcp.resource("schema://table/{table_name}")
def table_schema(table_name: str) -> str:
    """Return one table schema as JSON text."""
    try:
        payload = {"table": table_name, "columns": adapter.get_table_schema(table_name)}
    except ValidationError as exc:
        payload = {"ok": False, "error": str(exc)}
    return json.dumps(payload, indent=2)


if __name__ == "__main__":
    mcp.run()
