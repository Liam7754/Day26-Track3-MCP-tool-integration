# SQLite Lab MCP Server

## Goal

This project implements a FastMCP server backed by SQLite. It exposes exactly three tools:

- `search`
- `insert`
- `aggregate`

It also exposes schema resources at `schema://database` and `schema://table/{table_name}`.

## Project Structure

```text
implementation/
  db.py
  init_db.py
  mcp_server.py
  verify_server.py
  ui_app.py
  tests/
    test_server.py
requirements.txt
AGENTS.md
start_inspector.sh
```

The original `pseudocode/` files are kept as lab starter/reference material. The working implementation is in `implementation/`.

## Setup

Use Python 3.11 or newer. On this machine, `python3.11` was required because `fastmcp` was not installable with Python 3.9.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python implementation/init_db.py
```

If `python` points to an older interpreter:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python implementation/init_db.py
```

## Run Server

```bash
python implementation/mcp_server.py
```

The server runs on stdio by default.

## Run Tests

```bash
pytest
```

## Run Verification

```bash
python implementation/verify_server.py
```

The verification script resets the database, checks adapter behavior, discovers MCP tools/resources in-process with the FastMCP client, runs valid tool calls, and checks clear validation errors.

## Run UI Demo

The UI is optional presentation polish and uses the same `SQLiteAdapter` validation as the MCP server.

```bash
streamlit run implementation/ui_app.py
```

## MCP Inspector

From this repo:

```bash
npx -y @modelcontextprotocol/inspector /Users/tranvangiaban/Code/Day26-Track3-MCP-tool-integration/.venv/bin/python /Users/tranvangiaban/Code/Day26-Track3-MCP-tool-integration/implementation/mcp_server.py
```

Or:

```bash
./start_inspector.sh
```

## Codex Client Setup

Add this to `~/.codex/config.toml`:

```toml
[mcp_servers.sqlite_lab]
command = "python"
args = ["/Users/tranvangiaban/Code/Day26-Track3-MCP-tool-integration/implementation/mcp_server.py"]
```

If your shell needs the venv interpreter:

```toml
[mcp_servers.sqlite_lab]
command = "/Users/tranvangiaban/Code/Day26-Track3-MCP-tool-integration/.venv/bin/python"
args = ["/Users/tranvangiaban/Code/Day26-Track3-MCP-tool-integration/implementation/mcp_server.py"]
```

See `AGENTS.md` for the agent instruction:

```text
Use the `sqlite_lab` MCP server whenever the task needs database schema context or SQL-backed record lookup.
```

## Tools

### `search`

Input example:

```json
{
  "table": "students",
  "columns": ["id", "name", "cohort", "score"],
  "filters": {"cohort": "A1"},
  "order_by": "score",
  "descending": true,
  "limit": 20,
  "offset": 0
}
```

Output shape:

```json
{
  "ok": true,
  "rows": [{"id": 3, "name": "Chi Le", "cohort": "A1", "score": 97.0}],
  "metadata": {"table": "students", "row_count": 3, "limit": 20, "offset": 0}
}
```

Supported filter operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `like`, `in`.

### `insert`

Input example:

```json
{
  "table": "students",
  "values": {
    "name": "New Student",
    "cohort": "A1",
    "score": 90,
    "email": "new.student@example.edu"
  }
}
```

Output shape:

```json
{
  "ok": true,
  "data": {
    "id": 7,
    "name": "New Student",
    "cohort": "A1",
    "score": 90,
    "email": "new.student@example.edu",
    "created_at": "2026-05-14 10:00:00"
  },
  "metadata": {"table": "students"}
}
```

### `aggregate`

Input example:

```json
{
  "table": "students",
  "metric": "avg",
  "column": "score",
  "group_by": "cohort"
}
```

Output shape:

```json
{
  "ok": true,
  "rows": [{"cohort": "A1", "value": 90.83}],
  "metadata": {"table": "students", "metric": "avg", "column": "score", "group_by": "cohort"}
}
```

Supported metrics: `count`, `avg`, `sum`, `min`, `max`.

## Resources

Full database schema:

```text
schema://database
```

Single table schema:

```text
schema://table/students
```

Resources return JSON text with tables, columns, SQLite type, nullability, defaults, and primary key markers.

## Invalid Request Examples

Missing table:

```json
{"table": "missing_table"}
```

Response:

```json
{"ok": false, "error": "unknown table 'missing_table'", "metadata": {"error_type": "ValidationError"}}
```

Missing column:

```json
{"table": "students", "columns": ["password"]}
```

Bad operator:

```json
{"table": "students", "filters": {"score": {"between": [80, 90]}}}
```

## Demo Checklist

1. Start Inspector with the command above.
2. Show tools discovered: `search`, `insert`, `aggregate`.
3. Read `schema://database`.
4. Read `schema://table/students`.
5. Search all students in cohort `A1`.
6. Insert a new student.
7. Aggregate average score by cohort.
8. Show invalid missing table error.

## Verified Locally

These commands were run successfully with Python 3.11:

```bash
.venv/bin/python implementation/init_db.py
.venv/bin/python -m pytest
.venv/bin/python implementation/verify_server.py
.venv/bin/python implementation/mcp_server.py
```

Observed results:

- `pytest`: 13 passed
- `verify_server.py`: tools/resources discovered, valid MCP calls passed, invalid request returned clear error
- `mcp_server.py`: started FastMCP 3.2.4 on stdio without import errors
