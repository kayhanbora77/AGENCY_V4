"""Main Model Context Protocol (MCP) Server for Python, Polars, DuckDB, and Data Engineering."""

import logging
import sys
from typing import Optional
from mcp.server.mcpserver import MCPServer

from .config import DEFAULT_DB_PATH, WORKSPACE_ROOT
from .tools import (
    data_compare_schemas,
    data_quality_audit,
    data_scan_partitions,
    data_sniff_file,
    duckdb_describe_table,
    duckdb_execute,
    duckdb_explain,
    duckdb_export_data,
    duckdb_import_file,
    duckdb_list_tables,
    duckdb_query,
    polars_convert_format,
    polars_inspect_schema,
    polars_profile,
    polars_transform,
    python_execute,
    python_run_script,
)
from .utils import ensure_utf8_io, get_duckdb_conn

# Set up logging to stderr (stdio stdout is reserved for MCP JSON-RPC protocol)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("data-engineering-mcp")

# Initialize MCP Server
app = MCPServer(
    name="data-engineering-mcp",
    version="1.0.0",
    instructions=(
        "You have access to a high-performance Data Engineering MCP server with native support for "
        "Python, Polars, DuckDB, and ETL data pipelines.\n\n"
        "Available Capabilities:\n"
        "- DuckDB: Execute SQL, DDL/DML, inspect catalogs, explain query plans, and perform zero-copy Parquet/CSV import & export.\n"
        "- Polars: Zero-overhead schema scanning, comprehensive data profiling, streaming format conversions, and custom pipeline transformations.\n"
        "- Data Engineering: Sniff raw delimited files, run data hygiene audits, detect schema drift, and scan Hive partitions.\n"
        "- Python: Safe in-process data analysis execution with Polars/DuckDB/PyArrow/Pandas pre-imported, and external pipeline runner."
    ),
)

# ----------------------------------------------------------------------
# Register Tools Programmatically
# ----------------------------------------------------------------------
ALL_TOOLS = [
    # DuckDB Tools
    duckdb_query,
    duckdb_execute,
    duckdb_list_tables,
    duckdb_describe_table,
    duckdb_explain,
    duckdb_export_data,
    duckdb_import_file,
    # Polars Tools
    polars_profile,
    polars_inspect_schema,
    polars_transform,
    polars_convert_format,
    # Data Engineering Tools
    data_sniff_file,
    data_quality_audit,
    data_compare_schemas,
    data_scan_partitions,
    # Python Tools
    python_execute,
    python_run_script,
]

for tool_fn in ALL_TOOLS:
    app.tool()(tool_fn)


# ----------------------------------------------------------------------
# MCP Resources
# ----------------------------------------------------------------------
@app.resource("duckdb://tables", description="List of tables and views in the default DuckDB database")
def get_duckdb_tables_resource() -> str:
    """Return catalog of DuckDB tables and row counts."""
    return duckdb_list_tables()


@app.resource("workspace://environment", description="Overview of the current data engineering environment")
def get_environment_resource() -> str:
    """Return overview of workspace root, database path, and installed package versions."""
    import duckdb
    import pandas as pd
    import polars as pl
    import pyarrow as pa

    return (
        f"Data Engineering MCP Environment:\n"
        f"- Workspace Root: {WORKSPACE_ROOT}\n"
        f"- Default DuckDB Path: {DEFAULT_DB_PATH}\n"
        f"- DuckDB Version: {duckdb.__version__}\n"
        f"- Polars Version: {pl.__version__}\n"
        f"- PyArrow Version: {pa.__version__}\n"
        f"- Pandas Version: {pd.__version__}\n"
        f"- Python Interpreter: {sys.executable}\n"
    )


# ----------------------------------------------------------------------
# MCP Prompts
# ----------------------------------------------------------------------
@app.prompt(
    "dataset-hygiene-audit",
    description="Guided prompt to audit a dataset's format, delimiters, null ratios, and data hygiene.",
)
def prompt_dataset_audit(dataset_path_or_table: str) -> str:
    return (
        f"Please perform a thorough data hygiene and quality audit on '{dataset_path_or_table}'.\n"
        f"1. If it is a raw file, start by calling `data_sniff_file` to inspect encoding, delimiters, and sample lines.\n"
        f"2. Call `polars_profile` to compute summary statistics, null counts, and column types.\n"
        f"3. Run `data_quality_audit` to identify duplicates, empty string anomalies, and date parsing issues.\n"
        f"4. Provide a clear summary with concrete recommendations for schema cleaning and ingestion into DuckDB."
    )


@app.prompt(
    "optimize-duckdb-query",
    description="Diagnose and optimize a DuckDB SQL query using EXPLAIN and vectorized techniques.",
)
def prompt_optimize_query(sql_query: str) -> str:
    return (
        f"Analyze and optimize the following DuckDB SQL query:\n```sql\n{sql_query}\n```\n"
        f"1. Use `duckdb_explain` with analyze=True or analyze=False to inspect the execution plan.\n"
        f"2. Identify bottlenecks (e.g. unindexed full table scans, suboptimal join ordering, cross products).\n"
        f"3. Rewrite the query for optimal DuckDB vectorization and explain the improvements."
    )


# ----------------------------------------------------------------------
# Server Entry Point
# ----------------------------------------------------------------------
def main():
    """Run the MCP server over standard I/O (stdio)."""
    ensure_utf8_io()
    logger.info("Starting Data Engineering MCP Server (stdio transport)...")
    app.run(transport="stdio")


if __name__ == "__main__":
    main()
