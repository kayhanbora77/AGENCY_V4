# Data Engineering MCP Server
*(Python • Polars • DuckDB • Data Engineering Pipelines)*

A high-performance Model Context Protocol (MCP) server providing native, zero-copy analytics, ETL processing, and dataset hygiene tools for AI assistants.

---

## 🛠️ Feature Overview

### 1. 🦆 DuckDB Tools
Direct vectorized SQL analysis, DDL/DML execution, schema introspection, and high-speed Parquet/CSV transfers:
- **`duckdb_query`**: Execute read-only SQL queries on `.duckdb` files or in-memory (`:memory:`). Returns formatted Markdown tables, JSON, or CSV with timing metadata.
- **`duckdb_execute`**: Execute DDL/DML statements (`CREATE`, `INSERT`, `UPDATE`, `DROP`, `ALTER`, `COPY`).
- **`duckdb_list_tables`**: Catalog inspection listing tables, views, column counts, and exact/estimated row counts.
- **`duckdb_describe_table`**: Column names, DuckDB types, nullability, default values, and sample row previews.
- **`duckdb_explain`**: Physical/logical execution plan or `EXPLAIN ANALYZE` for query tuning.
- **`duckdb_export_data`**: High-speed native export of tables or queries to Parquet (with ZSTD compression), CSV, or JSON.
- **`duckdb_import_file`**: Zero-copy bulk import from CSV, Parquet, or JSON into DuckDB tables (`replace`, `append`, or `fail`).

### 2. ⚡ Polars Tools
Lightning-fast memory-efficient dataset profiling and streaming operations:
- **`polars_profile`**: Vectorized data profiling across files or DuckDB tables (calculates shape, memory, null counts & %, distinct values, min/max/mean, and flags data hygiene anomalies).
- **`polars_inspect_schema`**: Instant schema inspection using LazyFrame scanning without loading the entire dataset into memory.
- **`polars_transform`**: Execute custom Polars transformation logic on files or tables with automated result previews.
- **`polars_convert_format`**: Memory-bounded streaming format conversions (CSV, Parquet, Arrow/IPC, NDJSON).

### 3. 🏗️ Data Engineering Tools
Specialized tools to tackle real-world data pipelines and vendor data quirks:
- **`data_sniff_file`**: Robust sniffer for raw files: detects encodings (`utf-8`, `utf-8-sig`, `cp1252`, `latin-1`), delimiters (comma, tab, semicolon, pipe), quote characters, and header presence.
- **`data_quality_audit`**: Automated data quality audits detecting duplicate rows, primary key violations, whitespace-only strings masquerading as data, and date parsing failures across multiple formats.
- **`data_compare_schemas`**: Detects schema drift and type incompatibilities between two datasets or tables.
- **`data_scan_partitions`**: Scans Hive-partitioned directory trees (`year=.../month=...`), analyzing storage footprint, partition keys, and schema uniformity.

### 4. 🐍 Python Execution Tools
Interactive data science execution sandbox and script runner:
- **`python_execute`**: Run dynamic Python code in a data-engineering sandbox with `pl`, `duckdb`, `pa`, `pd`, and `Path` pre-loaded. Captures stdout, stderr, execution time, and formats returned DataFrames.
- **`python_run_script`**: Execute standalone data pipeline scripts (e.g. `create_table.py`) with full logging and exit status tracking.

---

## 📂 Project Structure

```
data_engineering_mcp/
├── __init__.py
├── config.py              # Environment variables, database paths, limits
├── utils.py               # UTF-8 safety, lock diagnostics, formatting
├── server.py              # MCP server initialization & tool registration
├── run_server.py          # CLI runner supporting stdio and SSE
├── tools/
│   ├── __init__.py
│   ├── duckdb_tools.py    # DuckDB SQL, DDL, catalog, explain, export/import
│   ├── polars_tools.py    # Polars profiling, transforms, conversions
│   ├── data_engineering_tools.py  # Sniffing, quality audits, schema diffs
│   └── python_tools.py    # Python execution sandbox & script runner
└── tests/
    └── test_server.py     # End-to-end verification test suite
```

---

## ⚙️ Configuration

Environment variables can be set in `.env` or system environment:

| Variable | Default | Description |
|---|---|---|
| `DUCKDB_PATH` | `C:\DuckDB\my_db.duckdb` | Default DuckDB database file path |
| `MCP_DEFAULT_ROW_LIMIT` | `100` | Default row count limit for SQL queries |
| `MCP_MAX_ROW_LIMIT` | `5000` | Hard upper bound on returned rows |
| `MCP_DEFAULT_SAMPLE_SIZE` | `10000` | Default sample size for Polars profiling |
| `MCP_SCRIPT_TIMEOUT` | `300` | Execution timeout (seconds) for external scripts |
| `MCP_SNIPPET_TIMEOUT` | `60` | Execution timeout (seconds) for dynamic Python snippets |

---

## 🚀 Running the Server

### 1. Via Stdio (Default for IDE / MCP Clients)
```powershell
.venv\Scripts\python.exe -m data_engineering_mcp.server
```

### 2. Via CLI Runner (Supports SSE for Remote/Web)
```powershell
# Stdio transport
.venv\Scripts\python.exe data_engineering_mcp\run_server.py --transport stdio

# SSE transport on port 8000
.venv\Scripts\python.exe data_engineering_mcp\run_server.py --transport sse --port 8000
```

---

## 🧪 Running Tests

Verify all 17 tools against local memory and temporary files:
```powershell
.venv\Scripts\python.exe data_engineering_mcp\tests\test_server.py
```
