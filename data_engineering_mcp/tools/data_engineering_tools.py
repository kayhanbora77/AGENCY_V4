"""Data Engineering MCP Tools: Delimited file sniffing, data quality auditing, schema comparison, and partition scanning."""

import csv
import io
import os
import re
import time
from pathlib import Path
from typing import Optional
import polars as pl
import duckdb

from .polars_tools import _load_polars_dataframe
from ..utils import format_dataframe, get_duckdb_conn, resolve_path, human_readable_bytes


def data_sniff_file(
    file_path: str,
    sample_lines: int = 30,
) -> str:
    """Sniff raw CSV/delimited files to detect encoding, delimiter, line terminators, headers, and quote characters.

    Args:
        file_path: Path to the raw text/CSV file to inspect.
        sample_lines: Number of lines to inspect for format detection (default 30).
    """
    p = resolve_path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")

    file_size = p.stat().st_size

    # Try common encodings
    detected_encoding = None
    raw_sample = None
    for enc in ["utf-8-sig", "utf-8", "cp1252", "latin-1"]:
        try:
            with open(p, "r", encoding=enc, errors="strict") as f:
                lines = [f.readline() for _ in range(sample_lines)]
                raw_sample = "".join(lines)
                detected_encoding = enc
                break
        except (UnicodeDecodeError, Exception):
            continue

    if not detected_encoding:
        detected_encoding = "latin-1 (fallback)"
        with open(p, "r", encoding="latin-1", errors="replace") as f:
            lines = [f.readline() for _ in range(sample_lines)]
            raw_sample = "".join(lines)

    # Sniff dialect
    delimiter = ","
    has_header = False
    quote_char = '"'
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(raw_sample)
        delimiter = dialect.delimiter
        quote_char = dialect.quotechar
        has_header = sniffer.has_header(raw_sample)
    except Exception:
        # Fallback heuristic for delimiter
        delims = {",": raw_sample.count(","), "\t": raw_sample.count("\t"), ";": raw_sample.count(";"), "|": raw_sample.count("|")}
        delimiter = max(delims, key=delims.get) if delims else ","

    delim_name = {",": "Comma (,)", "\t": "Tab (\\t)", ";": "Semicolon (;)", "|": "Pipe (|)"}.get(delimiter, f"'{delimiter}'")

    # Estimated line count
    avg_line_len = (len(raw_sample) / len(lines)) if lines else 80
    est_total_lines = int(file_size / avg_line_len) if avg_line_len > 0 else 0

    preview_lines = lines[:10]
    preview_block = "".join([f"{i+1:2d} | {ln}" for i, ln in enumerate(preview_lines)])

    return (
        f"### Raw File Inspection: `{p.name}`\n"
        f"- **File Path**: `{p}`\n"
        f"- **Total Size**: {human_readable_bytes(file_size)}\n"
        f"- **Detected Encoding**: `{detected_encoding}`\n"
        f"- **Detected Delimiter**: `{delim_name}`\n"
        f"- **Quote Character**: `{quote_char}`\n"
        f"- **Has Header Row (heuristic)**: `{has_header}`\n"
        f"- **Sample Line Count**: {len(lines)}\n"
        f"- **Estimated Total Lines**: ~{est_total_lines:,}\n\n"
        f"#### First {len(preview_lines)} Raw Lines Preview:\n"
        f"```text\n{preview_block}```\n\n"
        f"**Recommended Polars Read Configuration**:\n"
        f"```python\n"
        f"df = pl.read_csv(\n"
        f"    r'{p}',\n"
        f"    separator='{delimiter.replace(chr(9), '\\t')}',\n"
        f"    has_header={has_header},\n"
        f"    encoding='{detected_encoding}',\n"
        f")\n"
        f"```"
    )


def data_quality_audit(
    target: str,
    key_columns: Optional[list[str]] = None,
    date_columns: Optional[list[str]] = None,
    db_path: Optional[str] = None,
) -> str:
    """Run an automated Data Quality & Hygiene Audit on a dataset (nulls, blanks, duplicates, date validity).

    Args:
        target: File path (.csv, .parquet) or DuckDB table name.
        key_columns: Optional list of column names representing unique business keys to check for uniqueness.
        date_columns: Optional list of date/timestamp column names to validate format and parseability.
        db_path: DuckDB path if target is a database table.
    """
    df, source_desc = _load_polars_dataframe(target, db_path=db_path)
    total_rows = df.height
    total_cols = df.width

    findings = []
    warnings = []

    # 1. Total row and column count
    findings.append(f"- **Total Rows**: {total_rows:,}")
    findings.append(f"- **Total Columns**: {total_cols}")

    # 2. Duplicate detection
    exact_duplicates = total_rows - df.unique().height
    dup_pct = (exact_duplicates / total_rows * 100) if total_rows > 0 else 0
    findings.append(f"- **Exact Duplicate Rows**: {exact_duplicates:,} ({dup_pct:.2f}%)")
    if exact_duplicates > 0:
        warnings.append(f"⚠️ Found {exact_duplicates:,} completely identical duplicate rows.")

    # 3. Key column uniqueness
    if key_columns:
        valid_keys = [k for k in key_columns if k in df.columns]
        if valid_keys:
            key_dup_count = total_rows - df.select(valid_keys).unique().height
            findings.append(f"- **Key Duplicates on {valid_keys}**: {key_dup_count:,}")
            if key_dup_count > 0:
                warnings.append(f"❌ Key violation: {key_dup_count:,} duplicate records on key columns: {valid_keys}.")

    # 4. Check for hidden blank strings that should be null
    blank_cols = []
    for col in df.columns:
        if df[col].dtype == pl.Utf8 or df[col].dtype == pl.String:
            # Check for empty string or whitespace
            blank_count = df.filter(pl.col(col).str.strip_chars() == "").height
            if blank_count > 0:
                blank_cols.append(f"`{col}`: {blank_count:,} empty strings")
    if blank_cols:
        warnings.append(f"⚠️ Columns with un-trimmed empty strings instead of NULL: {', '.join(blank_cols)}.")

    # 5. Date validation if requested
    if date_columns:
        date_formats = [
            "%Y-%m-%d", "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y", "%d/%m/%Y %H:%M:%S",
            "%m/%d/%Y", "%m/%d/%Y %H:%M:%S",
        ]
        for dcol in date_columns:
            if dcol in df.columns and (df[dcol].dtype == pl.Utf8 or df[dcol].dtype == pl.String):
                total_original_nulls = df[dcol].null_count()
                parsed_series = df.select(
                    pl.coalesce([
                        pl.col(dcol).str.strip_chars().str.to_datetime(fmt, strict=False)
                        for fmt in date_formats
                    ]).alias("parsed")
                )["parsed"]
                unparseable = parsed_series.null_count() - total_original_nulls
                if unparseable > 0:
                    warnings.append(f"❌ Date parse failure in `{dcol}`: {unparseable:,} non-blank values failed to parse with standard formats.")

    # 6. Null percentages summary
    null_issues = []
    for col in df.columns:
        null_cnt = df[col].null_count()
        if null_cnt > 0:
            null_ratio = null_cnt / total_rows * 100
            if null_ratio > 30.0:
                null_issues.append(f"`{col}` ({null_ratio:.1f}% null)")
    if null_issues:
        warnings.append(f"ℹ️ High null columns (>30% null): {', '.join(null_issues)}.")

    output = [
        f"### Data Quality Audit: {source_desc}",
        "\n".join(findings),
        "",
        "#### Health Assessment",
    ]

    if warnings:
        output.extend([f"- {w}" for w in warnings])
    else:
        output.append("✅ **All data hygiene checks passed with zero issues!**")

    return "\n".join(output)


def data_compare_schemas(
    source: str,
    target: str,
    db_path: Optional[str] = None,
) -> str:
    """Compare schema definitions between two datasets (files or DuckDB tables) to detect drift or incompatibilities.

    Args:
        source: First dataset (file path or DuckDB table).
        target: Second dataset (file path or DuckDB table).
        db_path: DuckDB database path if referencing tables.
    """
    df_src, src_desc = _load_polars_dataframe(source, db_path=db_path, sample_rows=1)
    df_tgt, tgt_desc = _load_polars_dataframe(target, db_path=db_path, sample_rows=1)

    src_cols = {col: str(dtype) for col, dtype in zip(df_src.columns, df_src.dtypes)}
    tgt_cols = {col: str(dtype) for col, dtype in zip(df_tgt.columns, df_tgt.dtypes)}

    all_col_names = sorted(list(set(src_cols.keys()).union(tgt_cols.keys())))

    comparison_rows = []
    missing_in_target = []
    missing_in_source = []
    type_mismatches = []

    for col in all_col_names:
        src_type = src_cols.get(col, "*MISSING*")
        tgt_type = tgt_cols.get(col, "*MISSING*")

        if src_type == "*MISSING*":
            status = "Added in Target"
            missing_in_source.append(col)
        elif tgt_type == "*MISSING*":
            status = "Missing in Target"
            missing_in_target.append(col)
        elif src_type != tgt_type:
            status = "Type Mismatch"
            type_mismatches.append(f"`{col}` ({src_type} vs {tgt_type})")
        else:
            status = "Match"

        comparison_rows.append({
            "Column": col,
            f"Source ({src_desc[:12]})": src_type,
            f"Target ({tgt_desc[:12]})": tgt_type,
            "Status": status,
        })

    comp_df = pl.DataFrame(comparison_rows)

    summary = [
        f"### Schema Comparison",
        f"- **Source**: {src_desc} ({len(src_cols)} columns)",
        f"- **Target**: {tgt_desc} ({len(tgt_cols)} columns)",
        "",
    ]

    if missing_in_target:
        summary.append(f"⚠️ **Columns missing in Target**: {', '.join(missing_in_target)}")
    if missing_in_source:
        summary.append(f"ℹ️ **New columns added in Target**: {', '.join(missing_in_source)}")
    if type_mismatches:
        summary.append(f"❌ **Type Mismatches**: {', '.join(type_mismatches)}")
    if not missing_in_target and not missing_in_source and not type_mismatches:
        summary.append("✅ **Schemas are 100% identical and compatible!**")

    summary.append("")
    summary.append(format_dataframe(comp_df, format_type="table", max_rows=len(comparison_rows)))

    return "\n".join(summary)


def data_scan_partitions(
    directory_path: str,
    extension: str = "parquet",
) -> str:
    """Scan a partitioned directory structure (e.g. Hive partitioning year=.../month=...) and summarize partition keys, file sizes, and schema uniformity.

    Args:
        directory_path: Root folder of the partitioned dataset.
        extension: File extension to scan (default 'parquet').
    """
    root_dir = resolve_path(directory_path)
    if not root_dir.exists() or not root_dir.is_dir():
        raise NotADirectoryError(f"Directory not found: {root_dir}")

    ext_clean = extension.lstrip(".")
    matching_files = list(root_dir.rglob(f"*.{ext_clean}"))

    if not matching_files:
        return f"No `.{ext_clean}` files found under `{root_dir}`."

    total_size = sum(f.stat().st_size for f in matching_files)

    # Detect partition keys from folder paths (e.g. /year=2024/month=12/)
    partition_keys = set()
    sample_partitions = []
    for f in matching_files:
        rel = f.relative_to(root_dir)
        parts = rel.parts[:-1]
        p_dict = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                partition_keys.add(k)
                p_dict[k] = v
        if p_dict and len(sample_partitions) < 5:
            sample_partitions.append(p_dict)

    # Check schema consistency across first 3 files
    schema_samples = []
    for f in matching_files[:3]:
        try:
            if ext_clean in ["parquet", "pq"]:
                schema_samples.append(set(pl.scan_parquet(str(f)).collect_schema().names()))
            elif ext_clean in ["csv", "tsv"]:
                schema_samples.append(set(pl.scan_csv(str(f)).collect_schema().names()))
        except Exception:
            pass

    schema_consistent = True
    if len(schema_samples) > 1:
        first = schema_samples[0]
        schema_consistent = all(s == first for s in schema_samples[1:])

    report = [
        f"### Partition Scan for `{root_dir.name}`",
        f"- **Root Path**: `{root_dir}`",
        f"- **Total Files Found**: {len(matching_files):,}",
        f"- **Total Dataset Size**: {human_readable_bytes(total_size)}",
        f"- **Average File Size**: {human_readable_bytes(total_size / len(matching_files))}",
        f"- **Partition Keys Detected**: {list(partition_keys) if partition_keys else 'None (flat directory)'}",
        f"- **Schema Uniformity Across Slices**: {'✅ Consistent' if schema_consistent else '⚠️ Inconsistent columns detected!'}",
    ]

    if sample_partitions:
        report.append("\n#### Sample Partitions Detected:")
        for sp in sample_partitions:
            report.append(f"- `{sp}`")

    return "\n".join(report)
