"""Polars MCP Tools: Vectorized dataset profiling, streaming transformations, schema scanning, and format conversion."""

import io
import time
from pathlib import Path
from typing import Optional
import polars as pl
import duckdb

from ..config import DEFAULT_SAMPLE_SIZE
from ..utils import format_dataframe, get_duckdb_conn, resolve_path, human_readable_bytes


def _load_polars_dataframe(
    target: str,
    db_path: Optional[str] = None,
    sample_rows: Optional[int] = None,
) -> tuple[pl.DataFrame, str]:
    """Load a DataFrame from either a local file or a DuckDB table."""
    resolved = resolve_path(target)
    if resolved.exists() and resolved.is_file():
        suffix = resolved.suffix.lower()
        if suffix in [".parquet", ".pq"]:
            lf = pl.scan_parquet(str(resolved))
        elif suffix in [".csv", ".tsv", ".txt"]:
            lf = pl.scan_csv(str(resolved), ignore_errors=True)
        elif suffix in [".arrow", ".ipc", ".feather"]:
            lf = pl.scan_ipc(str(resolved))
        elif suffix in [".ndjson", ".jsonl"]:
            lf = pl.scan_ndjson(str(resolved))
        else:
            # Fallback to read_csv
            lf = pl.scan_csv(str(resolved), ignore_errors=True)

        if sample_rows:
            df = lf.limit(sample_rows).collect()
        else:
            df = lf.collect()
        return df, f"File: {resolved.name}"

    # If not a file, check if it's a DuckDB table name
    with get_duckdb_conn(db_path=db_path, read_only=True) as con:
        table_escaped = f'"{target.strip()}"'
        limit_clause = f" LIMIT {sample_rows}" if sample_rows else ""
        arrow_table = con.execute(f"SELECT * FROM {table_escaped}{limit_clause}").fetch_arrow_table()
        df = pl.from_arrow(arrow_table)
        return df, f"DuckDB Table: {target}"


def polars_inspect_schema(file_path: str) -> str:
    """Inspect schema and column types instantly without loading data into memory (using lazy scan).

    Args:
        file_path: Path to CSV, Parquet, IPC, or NDJSON file.
    """
    resolved = resolve_path(file_path)
    if not resolved.exists():
        raise FileNotFoundError(f"File not found: {resolved}")

    suffix = resolved.suffix.lower()
    start_time = time.perf_counter()

    if suffix in [".parquet", ".pq"]:
        lf = pl.scan_parquet(str(resolved))
        schema = lf.collect_schema()
    elif suffix in [".csv", ".tsv", ".txt"]:
        lf = pl.scan_csv(str(resolved))
        schema = lf.collect_schema()
    elif suffix in [".arrow", ".ipc", ".feather"]:
        lf = pl.scan_ipc(str(resolved))
        schema = lf.collect_schema()
    elif suffix in [".ndjson", ".jsonl"]:
        lf = pl.scan_ndjson(str(resolved))
        schema = lf.collect_schema()
    else:
        lf = pl.scan_csv(str(resolved))
        schema = lf.collect_schema()

    duration_ms = (time.perf_counter() - start_time) * 1000

    col_data = [{"Column Name": col, "Polars Type": str(dtype)} for col, dtype in schema.items()]
    df_schema = pl.DataFrame(col_data)

    file_size = resolved.stat().st_size
    output = [
        f"### Schema Scan for `{resolved.name}`",
        f"- Path: `{resolved}`",
        f"- File Size: {human_readable_bytes(file_size)}",
        f"- Total Columns: {len(col_data)}",
        f"- Scan Duration: {duration_ms:.2f} ms",
        "",
        format_dataframe(df_schema, format_type="table", max_rows=len(col_data)),
    ]
    return "\n".join(output)


def polars_profile(
    target: str,
    db_path: Optional[str] = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> str:
    """Compute a comprehensive data profile (nulls, types, cardinality, distribution, memory) for a file or DuckDB table.

    Args:
        target: File path (e.g. 'data/raw.csv') or DuckDB table name (e.g. 'BOBSALE_RAW').
        db_path: Path to DuckDB database if profiling a table.
        sample_size: Maximum rows to sample for profiling (default 10,000, 0 for entire dataset).
    """
    start_time = time.perf_counter()
    sample_limit = None if sample_size <= 0 else sample_size
    df, source_desc = _load_polars_dataframe(target, db_path=db_path, sample_rows=sample_limit)

    total_rows = df.height
    total_cols = df.width
    est_memory = df.estimated_size()

    profiles = []
    warnings = []

    for col in df.columns:
        series = df[col]
        dtype_str = str(series.dtype)
        null_count = series.null_count()
        null_pct = (null_count / total_rows * 100) if total_rows > 0 else 0.0

        n_unique = series.n_unique()
        unique_pct = (n_unique / total_rows * 100) if total_rows > 0 else 0.0

        # High level anomalies
        if null_count == total_rows:
            warnings.append(f"- Column `{col}` is 100% NULL.")
        elif n_unique == 1 and total_rows > 1:
            warnings.append(f"- Column `{col}` has only 1 constant distinct value: `{series[0]}`.")

        # Numeric stats or string stats
        min_val, max_val, mean_val = "N/A", "N/A", "N/A"
        try:
            if series.dtype.is_numeric():
                min_val = f"{series.min():.4g}" if series.min() is not None else "null"
                max_val = f"{series.max():.4g}" if series.max() is not None else "null"
                mean_val = f"{series.mean():.4g}" if series.mean() is not None else "null"
            elif series.dtype in [pl.Date, pl.Datetime]:
                min_val = str(series.min()) if series.min() is not None else "null"
                max_val = str(series.max()) if series.max() is not None else "null"
            else:
                # Text/other: display length range or sample
                non_nulls = series.drop_nulls()
                if non_nulls.len() > 0:
                    min_val = f"sample: {str(non_nulls[0])[:15]}"
                    max_val = f"sample: {str(non_nulls[-1])[:15]}"
        except Exception:
            pass

        profiles.append({
            "Column": col,
            "Type": dtype_str,
            "Nulls (%)": f"{null_pct:.1f}% ({null_count})",
            "Unique": f"{n_unique} ({unique_pct:.1f}%)",
            "Min": str(min_val),
            "Max": str(max_val),
            "Mean": str(mean_val),
        })

    prof_df = pl.DataFrame(profiles)
    duration_ms = (time.perf_counter() - start_time) * 1000

    report = [
        f"### Polars Data Profile: {source_desc}",
        f"- **Rows Sampled**: {total_rows:,} | **Columns**: {total_cols}",
        f"- **Estimated In-Memory Size**: {human_readable_bytes(est_memory)}",
        f"- **Profile Computation Time**: {duration_ms:.2f} ms",
        "",
        format_dataframe(prof_df, format_type="table", max_rows=total_cols),
    ]

    if warnings:
        report.append("\n#### Data Quality Alerts")
        report.extend(warnings)

    return "\n".join(report)


def polars_transform(
    code: str,
    input_file: Optional[str] = None,
    output_file: Optional[str] = None,
    table_name: Optional[str] = None,
    db_path: Optional[str] = None,
) -> str:
    """Execute a custom Polars transformation on a file or DuckDB table.

    The execution environment provides:
    - `pl`: polars library
    - `df`: loaded DataFrame (if input_file or table_name provided)
    - If code creates a new DataFrame, assign it to `df` or `result_df`.

    Args:
        code: Python/Polars code to execute.
        input_file: Optional input file path (CSV, Parquet, JSON, Arrow).
        output_file: Optional path to save resulting DataFrame (as .parquet or .csv).
        table_name: Optional DuckDB table name to load as starting DataFrame.
        db_path: Path to DuckDB database if loading a table.
    """
    exec_globals = {
        "pl": pl,
        "duckdb": duckdb,
    }

    start_source = None
    if input_file:
        df, start_source = _load_polars_dataframe(input_file)
        exec_globals["df"] = df
    elif table_name:
        df, start_source = _load_polars_dataframe(table_name, db_path=db_path)
        exec_globals["df"] = df

    start_time = time.perf_counter()
    exec_locals: dict = {}

    try:
        exec(code, exec_globals, exec_locals)
    except Exception as exc:
        raise RuntimeError(f"Polars transformation error: {exc}") from exc

    # Determine resulting DataFrame
    res_df = exec_locals.get("result_df", exec_locals.get("df", exec_globals.get("df")))
    if not isinstance(res_df, pl.DataFrame):
        return f"Code executed successfully in {(time.perf_counter() - start_time) * 1000:.2f} ms, but did not produce a Polars DataFrame (found {type(res_df)})."

    duration_ms = (time.perf_counter() - start_time) * 1000

    saved_msg = ""
    if output_file:
        out_path = resolve_path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        suffix = out_path.suffix.lower()
        if suffix in [".parquet", ".pq"]:
            res_df.write_parquet(str(out_path))
        elif suffix in [".csv", ".tsv"]:
            res_df.write_csv(str(out_path))
        elif suffix in [".ipc", ".arrow", ".feather"]:
            res_df.write_ipc(str(out_path))
        elif suffix in [".json", ".ndjson"]:
            res_df.write_ndjson(str(out_path))
        else:
            res_df.write_parquet(str(out_path))
        saved_msg = f"\n- **Saved to**: `{out_path}` ({human_readable_bytes(out_path.stat().st_size)})"

    return (
        f"**Polars Transformation Complete** ({duration_ms:.2f} ms)\n"
        f"- **Output Shape**: {res_df.height:,} rows x {res_df.width} columns"
        f"{saved_msg}\n\n"
        f"#### Result Preview (First 10 Rows)\n"
        f"{format_dataframe(res_df, format_type='table', max_rows=10)}"
    )


def polars_convert_format(
    input_path: str,
    output_path: str,
) -> str:
    """Streamingly convert between CSV, Parquet, IPC/Arrow, and NDJSON without high memory overhead.

    Args:
        input_path: Source file path.
        output_path: Destination file path (format inferred from file extension).
    """
    src = resolve_path(input_path)
    dst = resolve_path(output_path)
    dst.parent.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Input file not found: {src}")

    start_time = time.perf_counter()

    # Streaming read via scan
    src_ext = src.suffix.lower()
    if src_ext in [".parquet", ".pq"]:
        lf = pl.scan_parquet(str(src))
    elif src_ext in [".csv", ".tsv"]:
        lf = pl.scan_csv(str(src))
    elif src_ext in [".ipc", ".arrow", ".feather"]:
        lf = pl.scan_ipc(str(src))
    elif src_ext in [".ndjson", ".jsonl"]:
        lf = pl.scan_ndjson(str(src))
    else:
        lf = pl.scan_csv(str(src))

    # Streaming write where possible
    dst_ext = dst.suffix.lower()
    if dst_ext in [".parquet", ".pq"]:
        lf.sink_parquet(str(dst))
    elif dst_ext in [".csv", ".tsv"]:
        lf.sink_csv(str(dst))
    elif dst_ext in [".ipc", ".arrow", ".feather"]:
        lf.sink_ipc(str(dst))
    elif dst_ext in [".ndjson", ".jsonl"]:
        lf.sink_ndjson(str(dst))
    else:
        lf.sink_parquet(str(dst))

    duration_ms = (time.perf_counter() - start_time) * 1000
    src_size = src.stat().st_size
    dst_size = dst.stat().st_size
    ratio = (dst_size / src_size * 100) if src_size > 0 else 0

    return (
        f"**File Conversion Completed (Streaming)**\n"
        f"- Source: `{src}` ({human_readable_bytes(src_size)})\n"
        f"- Destination: `{dst}` ({human_readable_bytes(dst_size)})\n"
        f"- Size Ratio: {ratio:.1f}%\n"
        f"- Time Elapsed: {duration_ms:.2f} ms"
    )
