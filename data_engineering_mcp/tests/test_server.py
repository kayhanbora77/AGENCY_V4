"""Comprehensive tests for Data Engineering MCP Server tools and capabilities."""

import asyncio
import os
import sys
import tempfile
from pathlib import Path

# Ensure UTF-8 stdout
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import duckdb
import polars as pl

# Ensure parent directory is in path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from data_engineering_mcp.server import app, ALL_TOOLS
from data_engineering_mcp.tools import (
    duckdb_query,
    duckdb_execute,
    duckdb_list_tables,
    duckdb_describe_table,
    duckdb_explain,
    duckdb_export_data,
    duckdb_import_file,
    polars_profile,
    polars_inspect_schema,
    polars_transform,
    polars_convert_format,
    data_sniff_file,
    data_quality_audit,
    data_compare_schemas,
    data_scan_partitions,
    python_execute,
    python_run_script,
)


def test_tool_registration():
    """Verify all tools are registered on the MCPServer instance."""
    registered = asyncio.run(app.list_tools())
    tool_names = {t.name for t in registered}
    expected_names = {fn.__name__ for fn in ALL_TOOLS}
    assert expected_names.issubset(tool_names), f"Missing tools: {expected_names - tool_names}"
    print(f"Verified {len(tool_names)} registered tools.")


def test_duckdb_tools():
    """Verify DuckDB tools work properly on an in-memory database."""
    # Test execute
    exec_res = duckdb_execute(
        "CREATE TABLE test_users (id INTEGER, name VARCHAR, age INTEGER);",
        db_path=":memory:",
    )
    assert "Executed Successfully" in exec_res

    # Test insert and query
    with tempfile.TemporaryDirectory() as tmpdir:
        test_db = os.path.join(tmpdir, "test.duckdb")
        duckdb_execute("CREATE TABLE users (id INT, name VARCHAR, score DOUBLE);", db_path=test_db)
        duckdb_execute("INSERT INTO users VALUES (1, 'Alice', 95.5), (2, 'Bob', 82.0), (3, 'Charlie', 88.5);", db_path=test_db)

        # Query
        q_res = duckdb_query("SELECT * FROM users WHERE score > 85 ORDER BY score DESC;", db_path=test_db)
        assert "Alice" in q_res
        assert "Charlie" in q_res
        assert "Bob" not in q_res

        # List tables
        list_res = duckdb_list_tables(db_path=test_db)
        assert "users" in list_res

        # Describe table
        desc_res = duckdb_describe_table("users", db_path=test_db)
        assert "INTEGER" in desc_res or "INT" in desc_res
        assert "VARCHAR" in desc_res

        # Explain
        explain_res = duckdb_explain("SELECT count(*) FROM users;", db_path=test_db)
        assert "EXPLAIN" in explain_res

        # Export to Parquet
        pq_path = os.path.join(tmpdir, "users.parquet")
        export_res = duckdb_export_data("users", pq_path, format="parquet", db_path=test_db)
        assert os.path.exists(pq_path)
        assert "Export Completed" in export_res

        # Import to new table
        import_res = duckdb_import_file(pq_path, "users_imported", db_path=test_db)
        assert "Import Succeeded" in import_res
        q_imported = duckdb_query("SELECT count(*) as cnt FROM users_imported;", db_path=test_db)
        assert "3" in q_imported


def test_polars_and_data_engineering_tools():
    """Verify Polars and Data Engineering tools."""
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "sample.csv")
        df_src = pl.DataFrame({
            "flight_id": ["BA101", "LH202", "AF303", "BA101"],  # 1 duplicate key
            "date": ["2025-01-10", "2025-01-11", "2025-01-12", "2025-01-10"],
            "passengers": [180, 150, None, 180],
            "carrier": ["BA", "LH", "AF", "BA"],
            "notes": ["ok", " ", "delayed", "ok"],  # has empty string
        })
        df_src.write_csv(csv_path)

        # 1. File Sniffing
        sniff_res = data_sniff_file(csv_path)
        assert "Comma" in sniff_res
        assert "utf-8" in sniff_res.lower()

        # 2. Schema inspection
        schema_res = polars_inspect_schema(csv_path)
        assert "flight_id" in schema_res
        assert "passengers" in schema_res

        # 3. Profile
        prof_res = polars_profile(csv_path)
        assert "flight_id" in prof_res
        assert "passengers" in prof_res

        # 4. Data Quality Audit
        audit_res = data_quality_audit(
            csv_path,
            key_columns=["flight_id", "date"],
            date_columns=["date"],
        )
        assert "Duplicate" in audit_res

        # 5. Polars Transform
        code = "result_df = df.filter(pl.col('carrier') == 'BA')"
        transform_res = polars_transform(code, input_file=csv_path)
        assert "BA101" in transform_res

        # 6. Polars Format Conversion
        pq_path = os.path.join(tmpdir, "sample.parquet")
        conv_res = polars_convert_format(csv_path, pq_path)
        assert os.path.exists(pq_path)
        assert "Conversion Completed" in conv_res

        # 7. Compare Schemas
        csv2_path = os.path.join(tmpdir, "sample2.csv")
        df_src.drop("notes").write_csv(csv2_path)
        comp_res = data_compare_schemas(csv_path, csv2_path)
        assert "Missing in Target" in comp_res


def test_python_tools():
    """Verify python_execute runs with pre-imported data libraries."""
    code = """
df = pl.DataFrame({'x': [10, 20, 30], 'y': ['a', 'b', 'c']})
print("Calculated sum:", df['x'].sum())
result = df.filter(pl.col('x') >= 20)
"""
    res = python_execute(code)
    assert "Calculated sum: 60" in res
    assert "Polars DataFrame" in res
    assert "Status: ✅ OK" in res


if __name__ == "__main__":
    print("Running MCP test suite...")
    test_tool_registration()
    test_duckdb_tools()
    test_polars_and_data_engineering_tools()
    test_python_tools()
    print("\n[SUCCESS] ALL MCP DATA ENGINEERING TESTS PASSED SUCCESSFULLY!")
