from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from db import SQLiteAdapter, ValidationError
from init_db import create_database


DB_PATH = Path(__file__).resolve().parent / "lab.db"


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def verify_adapter() -> None:
    create_database(DB_PATH)
    adapter = SQLiteAdapter(DB_PATH)

    tables = adapter.list_tables()
    _assert(set(tables) == {"students", "courses", "enrollments"}, "expected demo tables")
    _assert(any(column["name"] == "cohort" for column in adapter.get_table_schema("students")), "schema visible")

    a1_rows = adapter.search("students", filters={"cohort": "A1"}, order_by="score", descending=True)
    _assert(len(a1_rows) == 3 and a1_rows[0]["name"] == "Chi Le", "valid search works")

    inserted = adapter.insert(
        "students",
        {"name": "Verifier Student", "cohort": "A1", "score": 89.0, "email": "verifier@example.edu"},
    )
    _assert("id" in inserted and inserted["cohort"] == "A1", "valid insert returns payload with id")

    avg_rows = adapter.aggregate("students", "avg", column="score", group_by="cohort")
    _assert(any(row["cohort"] == "A1" and row["value"] > 80 for row in avg_rows), "valid aggregate works")

    for label, action in {
        "invalid table": lambda: adapter.search("missing_table"),
        "invalid column": lambda: adapter.search("students", columns=["missing_column"]),
        "invalid aggregate": lambda: adapter.aggregate("students", "median", column="score"),
    }.items():
        try:
            action()
        except ValidationError as exc:
            print(f"PASS {label}: {exc}")
        else:
            raise AssertionError(f"{label} did not raise ValidationError")

    print("PASS adapter smoke checks")


async def verify_fastmcp_in_process() -> None:
    try:
        from fastmcp import Client
        from mcp_server import mcp
    except Exception as exc:
        print(f"SKIP FastMCP client smoke checks: {exc}")
        return

    async with Client(mcp) as client:
        tools = await client.list_tools()
        tool_names = {tool.name for tool in tools}
        _assert({"search", "insert", "aggregate"}.issubset(tool_names), "tools discoverable")
        print(f"PASS tools discoverable: {sorted(tool_names)}")

        resources = await client.list_resources()
        resource_uris = {str(resource.uri) for resource in resources}
        _assert("schema://database" in resource_uris, "database resource discoverable")
        print(f"PASS resources discoverable: {sorted(resource_uris)}")

        templates = await client.list_resource_templates()
        template_uris = {str(template.uriTemplate) for template in templates}
        _assert("schema://table/{table_name}" in template_uris, "table resource template discoverable")
        print(f"PASS resource templates discoverable: {sorted(template_uris)}")

        search_result = await client.call_tool(
            "search",
            {"table": "students", "filters": {"cohort": "A1"}, "order_by": "score", "descending": True},
        )
        payload = _content_to_json(search_result.content[0])
        _assert(payload["ok"] and payload["rows"], "MCP search call works")
        print("PASS MCP search call")

        insert_result = await client.call_tool(
            "insert",
            {
                "table": "students",
                "values": {
                    "name": "MCP Verifier",
                    "cohort": "C3",
                    "score": 86.0,
                    "email": "mcp.verifier@example.edu",
                },
            },
        )
        payload = _content_to_json(insert_result.content[0])
        _assert(payload["ok"] and "id" in payload["data"], "MCP insert call works")
        print("PASS MCP insert call")

        aggregate_result = await client.call_tool(
            "aggregate",
            {"table": "students", "metric": "avg", "column": "score", "group_by": "cohort"},
        )
        payload = _content_to_json(aggregate_result.content[0])
        _assert(payload["ok"] and payload["rows"], "MCP aggregate call works")
        print("PASS MCP aggregate call")

        invalid_result = await client.call_tool("search", {"table": "missing_table"})
        payload = _content_to_json(invalid_result.content[0])
        _assert(not payload["ok"] and "unknown table" in payload["error"], "MCP invalid table returns clear error")
        print("PASS MCP invalid request returns clear error")


def _content_to_json(content: Any) -> dict[str, Any]:
    if hasattr(content, "text"):
        return json.loads(content.text)
    if isinstance(content, dict):
        return content
    return json.loads(str(content))


def main() -> None:
    verify_adapter()
    asyncio.run(verify_fastmcp_in_process())
    print("Verification complete")


if __name__ == "__main__":
    main()
