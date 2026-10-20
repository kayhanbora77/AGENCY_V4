"""Configuration and connection utilities for Data Engineering MCP Server."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load local environment variables if available
load_dotenv()

# Workspace root
WORKSPACE_ROOT = Path(__file__).parent.parent.resolve()

# Default DuckDB database path
DEFAULT_DB_PATH = os.getenv("DUCKDB_PATH", r"C:\DuckDB\my_db.duckdb")

# Default query and profiling limits
DEFAULT_ROW_LIMIT = int(os.getenv("MCP_DEFAULT_ROW_LIMIT", "100"))
MAX_ROW_LIMIT = int(os.getenv("MCP_MAX_ROW_LIMIT", "5000"))
DEFAULT_SAMPLE_SIZE = int(os.getenv("MCP_DEFAULT_SAMPLE_SIZE", "10000"))

# Script execution timeout (seconds)
DEFAULT_SCRIPT_TIMEOUT = int(os.getenv("MCP_SCRIPT_TIMEOUT", "300"))
DEFAULT_SNIPPET_TIMEOUT = int(os.getenv("MCP_SNIPPET_TIMEOUT", "60"))
