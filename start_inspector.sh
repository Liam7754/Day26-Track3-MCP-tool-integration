#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON:-python}"

npx -y @modelcontextprotocol/inspector "$PYTHON_BIN" "$ROOT_DIR/implementation/mcp_server.py"
