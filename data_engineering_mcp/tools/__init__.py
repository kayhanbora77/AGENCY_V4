"""Tools package for Data Engineering MCP Server."""

from .duckdb_tools import (
    duckdb_describe_table,
    duckdb_execute,
    duckdb_explain,
    duckdb_export_data,
    duckdb_import_file,
    duckdb_list_tables,
    duckdb_query,
)
from .polars_tools import (
    polars_convert_format,
    polars_inspect_schema,
    polars_profile,
    polars_transform,
)
from .data_engineering_tools import (
    data_compare_schemas,
    data_quality_audit,
    data_scan_partitions,
    data_sniff_file,
)
from .python_tools import (
    python_execute,
    python_run_script,
)

__all__ = [
    # DuckDB
    "duckdb_query",
    "duckdb_execute",
    "duckdb_list_tables",
    "duckdb_describe_table",
    "duckdb_explain",
    "duckdb_export_data",
    "duckdb_import_file",
    # Polars
    "polars_profile",
    "polars_inspect_schema",
    "polars_transform",
    "polars_convert_format",
    # Data Engineering
    "data_sniff_file",
    "data_quality_audit",
    "data_compare_schemas",
    "data_scan_partitions",
    # Python
    "python_execute",
    "python_run_script",
]
