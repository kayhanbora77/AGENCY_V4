"""Utility functions for Data Engineering MCP Server: DB connection handling, formatting, path resolution."""

import io
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Optional
import duckdb
import polars as pl

from .config import DEFAULT_DB_PATH, WORKSPACE_ROOT


def ensure_utf8_io():
    """Ensure standard IO streams use UTF-8 on Windows."""
    if sys.platform == "win32":
        try:
            if hasattr(sys.stdout, "reconfigure"):
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
            if hasattr(sys.stderr, "reconfigure"):
                sys.stderr.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def resolve_path(path_str: str) -> Path:
    """Resolve a path relative to workspace root or as absolute."""
    p = Path(path_str).expanduser()
    if not p.is_absolute():
        p = (WORKSPACE_ROOT / p).resolve()
    return p


def human_readable_bytes(size: int | float) -> str:
    """Format bytes into human-readable string."""
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    val = float(size)
    while val >= 1024.0 and unit_index < len(units) - 1:
        val /= 1024.0
        unit_index += 1
    return f"{val:.2f} {units[unit_index]}"


@contextmanager
def get_duckdb_conn(
    db_path: Optional[str] = None, read_only: bool = True
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Context manager for DuckDB connection with helpful lock error diagnostics.

    If db_path is not specified or ':default', uses DEFAULT_DB_PATH.
    If ':memory:', creates a temporary in-memory database.
    """
    target_path = db_path
    if not target_path or target_path == ":default":
        target_path = DEFAULT_DB_PATH

    is_memory = target_path == ":memory:"

    try:
        if is_memory:
            con = duckdb.connect(database=":memory:")
        else:
            p = resolve_path(target_path)
            # Create parent folder if writing to a new file
            if not read_only and not p.parent.exists():
                p.parent.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(database=str(p), read_only=read_only)
    except duckdb.IOException as exc:
        err_msg = str(exc)
        if "used by another process" in err_msg or "Cannot open file" in err_msg:
            raise RuntimeError(
                f"DuckDB database at '{target_path}' is currently locked by another application (e.g. DBeaver, another Python process).\n"
                f"Original error: {err_msg}\n"
                f"Suggestions:\n"
                f"1. Close the active connection in DBeaver or disconnect the database.\n"
                f"2. Or specify an in-memory database (':memory:') or alternative copy."
            ) from exc
        raise
    except Exception as exc:
        raise RuntimeError(f"Failed to connect to DuckDB at '{target_path}': {exc}") from exc

    try:
        yield con
    finally:
        con.close()


def dataframe_to_markdown_table(df: pl.DataFrame, max_rows: int = 100) -> str:
    """Convert a Polars DataFrame into a GitHub-flavored Markdown table."""
    if df.height == 0:
        return "*Empty DataFrame (0 rows)*"

    truncated = False
    display_df = df
    if df.height > max_rows:
        display_df = df.head(max_rows)
        truncated = True

    columns = display_df.columns
    header_row = "| " + " | ".join(columns) + " |"
    separator_row = "| " + " | ".join(["---"] * len(columns)) + " |"

    data_rows = []
    for row in display_df.iter_rows():
        cells = []
        for cell in row:
            if cell is None:
                cells.append("*null*")
            else:
                c_str = str(cell).replace("\n", " ").replace("|", "\\|")
                if len(c_str) > 120:
                    c_str = c_str[:117] + "..."
                cells.append(c_str)
        data_rows.append("| " + " | ".join(cells) + " |")

    table_md = "\n".join([header_row, separator_row] + data_rows)
    if truncated:
        table_md += f"\n\n*(Showing first {max_rows} of {df.height} rows)*"
    return table_md


def format_dataframe(
    df: pl.DataFrame, format_type: str = "table", max_rows: int = 100
) -> str:
    """Format DataFrame into the requested output format."""
    f_lower = format_type.lower().strip()
    if f_lower == "json":
        rows = df.head(max_rows).to_dicts()
        return json.dumps(rows, indent=2, default=str)
    elif f_lower == "csv":
        buf = io.StringIO()
        df.head(max_rows).write_csv(buf)
        return buf.getvalue()
    elif f_lower == "polars":
        return str(df.head(max_rows))
    else:  # default "table"
        return dataframe_to_markdown_table(df, max_rows=max_rows)
