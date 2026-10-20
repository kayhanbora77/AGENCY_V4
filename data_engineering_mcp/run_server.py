"""CLI Entry point for Data Engineering MCP Server."""

import argparse
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_engineering_mcp.config import DEFAULT_DB_PATH
from data_engineering_mcp.server import app, logger
from data_engineering_mcp.utils import ensure_utf8_io


def main():
    parser = argparse.ArgumentParser(description="Data Engineering MCP Server (Python, Polars, DuckDB)")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport protocol (stdio for local client, sse for remote/web)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind if using SSE transport (default 8000)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind if using SSE transport (default 127.0.0.1)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help=f"Path to default DuckDB file (defaults to DUCKDB_PATH or {DEFAULT_DB_PATH})",
    )

    args = parser.parse_args()

    ensure_utf8_io()

    if args.db_path:
        import os
        os.environ["DUCKDB_PATH"] = args.db_path

    if args.transport == "stdio":
        logger.info("Starting Data Engineering MCP server on stdio...")
        app.run(transport="stdio")
    elif args.transport == "sse":
        logger.info(f"Starting Data Engineering MCP server on SSE (http://{args.host}:{args.port})...")
        app.run(transport="sse", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
