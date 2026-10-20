"""DuckDB MCP Tools: Querying, DDL/DML execution, schema inspection, export/import, and query planning."""

import time
from pathlib import Path
from typing import Optional
import duckdb
import polars as pl

from ..config import DEFAULT_ROW_LIMIT, MAX_ROW_LIMIT
from ..utils import format_dataframe, get_duckdb_conn, resolve_path, human_readable_bytes


def duckdb_query(
    query: str,
    db_path: Optional[str] = None,
    limit: int = DEFAULT_ROW_LIMIT,
    format: str = "table",
) -> str:
    """Execute a read-only SQL query against DuckDB and return formatted results.

    Args:
        query: SQL query string to execute (e.g. 'SELECT * FROM my_table WHERE age > 30').
        db_path: Path to .duckdb database file, ':default', or ':memory:'. If omitted, uses default DB.
        limit: Maximum number of rows to return (default 100, max 5000).
        format: Output format ('table' for markdown table, 'json' for JSON array, 'csv' for CSV text).
    """
    row_limit = min(max(1, limit), MAX_ROW_LIMIT)
    clean_query = query.strip()

    # Disallow write statements in read-only query tool
    disallowed = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
    first_token = clean_query.split()[0].upper() if clean_query else ""
    if first_token in disallowed:
        return f"Error: '{first_token}' statement is not allowed in duckdb_query. Use duckdb_execute for DDL/DML statements."

    start_time = time.perf_counter()
    with get_duckdb_conn(db_path=db_path, read_only=True) as con:
        # Run query and retrieve as Polars DataFrame
        res = con.execute(clean_query)
        total_rows = None
        try:
            # Try fetching as Arrow table then Polars DataFrame for maximum zero-copy speed
            arrow_table = res.fetch_arrow_table()
            total_rows = arrow_table.num_rows
            df = pl.from_arrow(arrow_table)
        except Exception:
            # Fallback to fetching records
            columns = [desc[0] for desc in res.description] if res.description else []
            rows = res.fetchall()
            total_rows = len(rows)
            df = pl.DataFrame(rows, schema=columns, orient="row")

    duration_ms = (time.perf_counter() - start_time) * 1000

    output = []
    output.append(f"**DuckDB Query Executed** in {duration_ms:.2f} ms")
    output.append(f"Total Rows: {total_rows} | Columns: {len(df.columns)}")
    output.append("")
    output.append(format_dataframe(df, format_type=format, max_rows=row_limit))

    return "\n".join(output)


def duckdb_execute(
    sql: str,
    db_path: Optional[str] = None,
) -> str:
    """Execute a DDL or DML statement (CREATE, INSERT, UPDATE, DROP, ALTER, COPY) in DuckDB.

    Args:
        sql: SQL statement to execute.
        db_path: Path to .duckdb database file, ':default', or ':memory:'.
    """
    clean_sql = sql.strip()
    start_time = time.perf_counter()
    with get_duckdb_conn(db_path=db_path, read_only=False) as con:
        res = con.execute(clean_sql)
        try:
            affected = res.fetchall()
            rows_affected = len(affected) if affected else 0
        except Exception:
            rows_affected = 0

    duration_ms = (time.perf_counter() - start_time) * 1000
    return (
        f"**DuckDB Statement Executed Successfully**\n"
        f"- Execution time: {duration_ms:.2f} ms\n"
        f"- Status: OK\n"
        f"- Query preview: `{clean_sql[:120]}{'...' if len(clean_sql) > 120 else ''}`"
    )


def duckdb_list_tables(
    db_path: Optional[str] = None,
) -> str:
    """List all schemas, tables, and views in the DuckDB database along with row counts and column counts.

    Args:
        db_path: Path to .duckdb database file, ':default', or ':memory:'.
    """
    with get_duckdb_conn(db_path=db_path, read_only=True) as con:
        query = """
            SELECT 
                table_schema AS schema_name,
                table_name,
                table_type,
                (SELECT count(*) FROM information_schema.columns c 
                 WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) AS column_count
            FROM information_schema.tables t
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name;
        """
        tables = con.execute(query).fetchall()

        if not tables:
            return "No user tables or views found in the DuckDB database."

        # Fetch actual or estimated row count for each table
        rows_data = []
        for schema_name, table_name, table_type, col_count in tables:
            try:
                cnt = con.execute(f'SELECT count(*) FROM "{schema_name}"."{table_name}"').fetchone()[0]
                row_str = f"{cnt:,}"
            except Exception:
                row_str = "N/A"
            rows_data.append({
                "Schema": schema_name,
                "Table Name": table_name,
                "Type": table_type,
                "Columns": col_count,
                "Row Count": row_str,
            })

    df = pl.DataFrame(rows_data)
    md_table = format_dataframe(df, format_type="table")
    return f"### DuckDB Tables & Views ({len(rows_data)} total)\n\n" + md_table


def duckdb_describe_table(
    table_name: str,
    db_path: Optional[str] = None,
) -> str:
    """Get detailed schema information (column names, types, nullability, defaults) and sample rows for a table.

    Args:
        table_name: Name of the table or view to describe.
        db_path: Path to .duckdb database file, ':default', or ':memory:'.
    """
    clean_name = table_name.strip()
    with get_duckdb_conn(db_path=db_path, read_only=True) as con:
        # Schema info
        desc_query = f'DESCRIBE "{clean_name}"'
        try:
            desc_df = con.execute(desc_query).pl()
        except Exception as exc:
            # Try without quotes or with schema
            desc_df = con.execute(f"DESCRIBE {clean_name}").pl()

        # Sample rows
        sample_query = f'SELECT * FROM "{clean_name}" LIMIT 3'
        try:
            sample_df = con.execute(sample_query).pl()
            total_count = con.execute(f'SELECT count(*) FROM "{clean_name}"').fetchone()[0]
        except Exception:
            sample_df = con.execute(f"SELECT * FROM {clean_name} LIMIT 3").pl()
            total_count = con.execute(f"SELECT count(*) FROM {clean_name}").fetchone()[0]

    output = [
        f"### Schema for Table: `{clean_name}` (Total Rows: {total_count:,})",
        "",
        "#### Columns & Types",
        format_dataframe(desc_df, format_type="table"),
        "",
        "#### Sample Preview (First 3 Rows)",
        format_dataframe(sample_df, format_type="table"),
    ]
    return "\n".join(output)


def duckdb_explain(
    query: str,
    db_path: Optional[str] = None,
    analyze: bool = False,
) -> str:
    """Generate physical and logical execution plans to diagnose query performance in DuckDB.

    Args:
        query: SQL query to explain.
        db_path: Path to .duckdb database file, ':default', or ':memory:'.
        analyze: If True, actually executes the query and returns actual timings and operator statistics (EXPLAIN ANALYZE).
    """
    prefix = "EXPLAIN ANALYZE " if analyze else "EXPLAIN "
    full_query = prefix + query.strip()

    with get_duckdb_conn(db_path=db_path, read_only=False) as con:
        res = con.execute(full_query).fetchall()

    plan_lines = [r[1] if len(r) > 1 else str(r[0]) for r in res]
    plan_text = "\n".join(plan_lines)

    return f"```\n=== DuckDB Query Plan ({'ANALYZE' if analyze else 'EXPLAIN'}) ===\n{plan_text}\n```"


def duckdb_export_data(
    query_or_table: str,
    output_path: str,
    format: str = "parquet",
    compression: str = "zstd",
    db_path: Optional[str] = None,
) -> str:
    """Export a DuckDB table or SQL query directly to Parquet, CSV, or JSON using DuckDB's high-speed native writer.

    Args:
        query_or_table: Either a table name (e.g. 'BOBSALE_RAW') or a full SELECT query.
        output_path: Destination file path (e.g. 'exports/flights.parquet').
        format: Export file format ('parquet', 'csv', 'json').
        compression: Compression codec for parquet/csv (e.g. 'zstd', 'snappy', 'gzip', 'none').
        db_path: Path to .duckdb database file, ':default', or ':memory:'.
    """
    target_out = resolve_path(output_path)
    target_out.parent.mkdir(parents=True, exist_ok=True)

    src = query_or_table.strip()
    select_clause = src if src.upper().startswith("SELECT") else f'SELECT * FROM "{src}"'

    fmt_upper = format.upper().strip()
    compression_opt = f", COMPRESSION '{compression}'" if fmt_upper in ["PARQUET", "CSV"] and compression else ""

    copy_sql = f"COPY ({select_clause}) TO '{str(target_out).replace('\\', '/')}' (FORMAT {fmt_upper}{compression_opt});"

    start_time = time.perf_counter()
    with get_duckdb_conn(db_path=db_path, read_only=False) as con:
        con.execute(copy_sql)
    duration_ms = (time.perf_counter() - start_time) * 1000

    size_bytes = target_out.stat().st_size if target_out.exists() else 0
    return (
        f"**DuckDB Export Completed**\n"
        f"- Target File: `{target_out}`\n"
        f"- File Size: {human_readable_bytes(size_bytes)}\n"
        f"- Format: {fmt_upper} (compression: {compression})\n"
        f"- Time Elapsed: {duration_ms:.2f} ms"
    )


def duckdb_import_file(
    file_path: str,
    table_name: str,
    file_type: str = "auto",
    if_exists: str = "replace",
    db_path: Optional[str] = None,
) -> str:
    """Direct high-speed import of CSV, Parquet, or JSON file into DuckDB.

    Args:
        file_path: Path to the input file (.csv, .tsv, .parquet, .json).
        table_name: Name of the destination DuckDB table.
        file_type: 'auto', 'csv', 'parquet', or 'json'.
        if_exists: 'replace' (drop and recreate), 'append' (insert into existing), or 'fail'.
        db_path: Path to .duckdb database file, ':default', or ':memory:'.
    """
    in_file = resolve_path(file_path)
    if not in_file.exists():
        raise FileNotFoundError(f"Input file not found at: {in_file}")

    in_path_escaped = str(in_file).replace("\\", "/")
    table_escaped = f'"{table_name.strip()}"'

    # Auto detect file type
    ftype = file_type.lower()
    if ftype == "auto":
        suffix = in_file.suffix.lower()
        if suffix in [".parquet", ".pq"]:
            ftype = "parquet"
        elif suffix in [".json", ".jsonl", ".ndjson"]:
            ftype = "json"
        else:
            ftype = "csv"

    scan_expr = {
        "parquet": f"read_parquet('{in_path_escaped}')",
        "csv": f"read_csv_auto('{in_path_escaped}', header=True, sample_size=20000)",
        "json": f"read_json_auto('{in_path_escaped}')",
    }.get(ftype, f"read_csv_auto('{in_path_escaped}')")

    start_time = time.perf_counter()
    with get_duckdb_conn(db_path=db_path, read_only=False) as con:
        if if_exists == "replace":
            con.execute(f"DROP TABLE IF EXISTS {table_escaped};")
            con.execute(f"CREATE TABLE {table_escaped} AS SELECT * FROM {scan_expr};")
        elif if_exists == "append":
            con.execute(f"INSERT INTO {table_escaped} SELECT * FROM {scan_expr};")
        elif if_exists == "fail":
            con.execute(f"CREATE TABLE {table_escaped} AS SELECT * FROM {scan_expr};")
        else:
            raise ValueError(f"Unknown if_exists mode: {if_exists}")

        row_count = con.execute(f"SELECT count(*) FROM {table_escaped}").fetchone()[0]

    duration_ms = (time.perf_counter() - start_time) * 1000
    return (
        f"**DuckDB File Import Succeeded**\n"
        f"- Imported From: `{in_file}`\n"
        f"- Target Table: `{table_name}`\n"
        f"- Total Rows in Table: {row_count:,}\n"
        f"- Mode: {if_exists}\n"
        f"- Time Elapsed: {duration_ms:.2f} ms"
    )
