"""Python MCP Tools: In-process interactive data science execution and standalone script runner."""

import datetime
import io
import json
import math
import os
import re
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Optional
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa

from ..config import DEFAULT_SCRIPT_TIMEOUT, DEFAULT_SNIPPET_TIMEOUT, WORKSPACE_ROOT
from ..utils import format_dataframe, resolve_path, human_readable_bytes


def python_execute(
    code: str,
    timeout_seconds: int = DEFAULT_SNIPPET_TIMEOUT,
) -> str:
    """Execute dynamic Python code in a rich data engineering environment.

    Pre-loaded modules:
    - `pl`: polars
    - `duckdb`: duckdb
    - `pd`: pandas
    - `pa`: pyarrow
    - `Path`: pathlib.Path
    - `datetime`, `json`, `re`, `math`, `os`, `sys`

    If your code defines `result` or `df`, it will be formatted and returned in the summary.

    Args:
        code: Python code snippet to execute.
        timeout_seconds: Maximum allowed execution time in seconds (default 60).
    """
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    exec_globals = {
        "pl": pl,
        "duckdb": duckdb,
        "pd": pd,
        "pa": pa,
        "Path": Path,
        "datetime": datetime,
        "json": json,
        "re": re,
        "math": math,
        "os": os,
        "sys": sys,
    }
    exec_locals: dict = {}

    old_stdout = sys.stdout
    old_stderr = sys.stderr

    start_time = time.perf_counter()
    error_msg = None

    try:
        sys.stdout = stdout_buf
        sys.stderr = stderr_buf
        exec(code, exec_globals, exec_locals)
    except Exception:
        error_msg = traceback.format_exc()
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr

    duration_ms = (time.perf_counter() - start_time) * 1000

    out_text = stdout_buf.getvalue().strip()
    err_text = stderr_buf.getvalue().strip()

    response = [
        f"**Python Execution Completed** in {duration_ms:.2f} ms",
        f"- Status: {'❌ Error' if error_msg else '✅ OK'}",
    ]

    if out_text:
        response.append("\n#### Standard Output:")
        response.append(f"```\n{out_text}\n```")

    if err_text:
        response.append("\n#### Standard Error:")
        response.append(f"```\n{err_text}\n```")

    if error_msg:
        response.append("\n#### Traceback:")
        response.append(f"```\n{error_msg}\n```")

    # Inspect returned or assigned variables of interest
    target_obj = exec_locals.get("result", exec_locals.get("df", None))
    if target_obj is not None:
        response.append("\n#### Result Summary:")
        if isinstance(target_obj, pl.DataFrame):
            response.append(f"Polars DataFrame: {target_obj.height} rows x {target_obj.width} columns")
            response.append(format_dataframe(target_obj, format_type="table", max_rows=15))
        elif isinstance(target_obj, pd.DataFrame):
            response.append(f"Pandas DataFrame: {len(target_obj)} rows x {len(target_obj.columns)} columns")
            response.append(format_dataframe(pl.from_pandas(target_obj), format_type="table", max_rows=15))
        elif isinstance(target_obj, (dict, list)):
            try:
                response.append(f"```json\n{json.dumps(target_obj, indent=2, default=str)[:2000]}\n```")
            except Exception:
                response.append(f"```\n{repr(target_obj)[:1000]}\n```")
        else:
            response.append(f"`{repr(target_obj)}`")

    return "\n".join(response)


def python_run_script(
    script_path: str,
    args: Optional[list[str]] = None,
    timeout_seconds: int = DEFAULT_SCRIPT_TIMEOUT,
) -> str:
    """Run an external Python data pipeline script (e.g. create_table.py) using the workspace virtual environment.

    Args:
        script_path: Relative or absolute path to the Python script file.
        args: Optional list of CLI arguments to pass to the script.
        timeout_seconds: Execution timeout in seconds (default 300).
    """
    resolved_script = resolve_path(script_path)
    if not resolved_script.exists():
        raise FileNotFoundError(f"Script file not found: {resolved_script}")

    # Determine Python interpreter to use
    venv_py = WORKSPACE_ROOT / ".venv" / "Scripts" / "python.exe"
    if not venv_py.exists():
        venv_py = Path(sys.executable)

    cmd = [str(venv_py), str(resolved_script)]
    if args:
        cmd.extend(args)

    start_time = time.perf_counter()
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(resolved_script.parent),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            env=env,
        )
        duration_s = time.perf_counter() - start_time

        result = [
            f"### Script Execution: `{resolved_script.name}`",
            f"- **Interpreter**: `{venv_py}`",
            f"- **Exit Code**: {proc.returncode} ({'✅ Success' if proc.returncode == 0 else '❌ Failed'})",
            f"- **Execution Time**: {duration_s:.2f} s",
        ]

        if proc.stdout.strip():
            result.append("\n#### Output (stdout):")
            result.append(f"```\n{proc.stdout.strip()}\n```")

        if proc.stderr.strip():
            result.append("\n#### Errors (stderr):")
            result.append(f"```\n{proc.stderr.strip()}\n```")

        return "\n".join(result)

    except subprocess.TimeoutExpired:
        return f"❌ Script `{resolved_script.name}` exceeded the timeout of {timeout_seconds} seconds and was terminated."
    except Exception as exc:
        return f"❌ Failed to run script `{resolved_script.name}`: {exc}"
