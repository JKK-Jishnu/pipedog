"""
profiler.py — File loading, type inference, statistical profiling, and snapshot I/O.

This module is the core of `pipedog init`. It is responsible for:
  1. Reading CSV, Parquet, and JSON files into a pandas DataFrame.
  2. Inferring a human-readable type for every column (integer, float, string,
     boolean, datetime).
  3. Computing per-column statistics (null counts, unique counts, value ranges).
  4. Auto-generating quality rules from those statistics.
  5. Persisting the snapshot and rules to .pipedog/ so `pipedog scan` can
     load them later.

Snapshot layout on disk:
    .pipedog/
        schema.json   — DataSchema (row count, column stats, timestamp)
        checks.json   — QualityChecks (auto-generated rules)
"""

from __future__ import annotations

import json
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from .schema import ColumnSchema, DataSchema, QualityCheck, QualityChecks

# Directory and file names used for the on-disk snapshot.
PIPEDOG_DIR = ".pipedog"
SCHEMA_FILE = "schema.json"
CHECKS_FILE = "checks.json"

# Raw pandas dtype prefixes that map to numeric columns.
# Used as a guard before attempting min/max/mean calculations.
NUMERIC_DTYPES = {"int64", "float64", "int32", "float32", "int16", "int8"}


def _pipedog_dir() -> Path:
    """Return the Path object for the .pipedog snapshot directory."""
    return Path(PIPEDOG_DIR)


def load_file(file_path: str) -> pd.DataFrame:
    """
    Read a data file into a pandas DataFrame.

    Dispatches to the correct pandas reader based on file extension.
    Raises ValueError for unsupported extensions so the caller can show a
    friendly error message instead of a raw pandas exception.

    Supported extensions:
        .csv             — read_csv (infers delimiter and dtypes automatically)
        .parquet / .pq   — read_parquet (requires pyarrow to be installed)
        .json            — read_json (expects a JSON array or records orientation)

    Args:
        file_path: Relative or absolute path to the data file.

    Returns:
        A pandas DataFrame with all columns and rows from the file.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError:        If the file extension is not supported.
    """
    path = Path(file_path)
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(file_path)
    elif ext in (".parquet", ".pq"):
        return pd.read_parquet(file_path)
    elif ext == ".json":
        return pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file type: '{ext}'. Supported: .csv, .parquet, .json")


def _dtype_name(series: pd.Series) -> str:
    """
    Map a pandas Series to a human-readable Pipedog type string.

    Pipedog collapses the many pandas numeric subtypes (int8, int32, int64,
    float32, float64…) into simple labels that analysts can understand.
    For object (string) columns it heuristically probes whether the values
    look like dates by attempting pd.to_datetime on a small sample.

    Type mapping:
        int* dtype                     → "integer"
        float* dtype                   → "float"
        bool dtype                     → "boolean"
        object dtype + parseable dates → "datetime"
        object dtype + other strings   → "string"
        datetime64[*] dtype            → "datetime"
        anything else                  → raw dtype string (fallback)

    Args:
        series: A single column from a pandas DataFrame.

    Returns:
        A short type label string.
    """
    dtype = str(series.dtype)
    if dtype.startswith("int"):
        return "integer"
    if dtype.startswith("float"):
        return "float"
    if dtype == "bool":
        return "boolean"
    if dtype == "object":
        # Heuristic: try to parse the first 5 non-null values as dates.
        # If it succeeds, treat the whole column as datetime.
        # Warnings are suppressed because pandas emits a noisy UserWarning
        # when it falls back to dateutil for ambiguous formats.
        sample = series.dropna().head(5)
        if len(sample) > 0:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    pd.to_datetime(sample)
                return "datetime"
            except Exception:
                pass
        return "string"
    if "datetime" in dtype:
        return "datetime"
    # Fallback: return the raw dtype string for unexpected types.
    return dtype


def profile_dataframe(df: pd.DataFrame, file_path: str) -> DataSchema:
    """
    Compute per-column statistics for a DataFrame and return a DataSchema.

    For each column this function records:
      - The inferred Pipedog type (via _dtype_name).
      - Null count and null percentage.
      - Distinct value count (excluding nulls).
      - Up to 5 non-null sample values (JSON-safe; non-serialisable values
        are converted to strings).
      - Numeric min, max, and mean (only for integer/float columns).

    The returned DataSchema is the canonical "snapshot" used by both the
    `init` command (to persist a baseline) and the `scan` command (to
    describe the current file for comparison).

    Args:
        df:        The DataFrame to profile.
        file_path: Original path to the source file (stored in the snapshot
                   as an absolute path for traceability).

    Returns:
        A DataSchema containing metadata and per-column statistics.
    """
    columns: list[ColumnSchema] = []

    for col in df.columns:
        series = df[col]
        total = len(series)

        null_count = int(series.isna().sum())
        null_pct = round(null_count / total * 100, 2) if total > 0 else 0.0
        unique_count = int(series.nunique(dropna=True))
        dtype = _dtype_name(series)

        # Collect sample values, converting anything that isn't JSON-safe
        # (e.g. numpy scalars, Timestamps) to plain strings.
        sample_raw = series.dropna().head(5).tolist()
        sample_values: list[Any] = []
        for v in sample_raw:
            try:
                json.dumps(v)
                sample_values.append(v)
            except (TypeError, ValueError):
                sample_values.append(str(v))

        # Numeric stats are only meaningful for integer and float columns.
        min_val = max_val = mean_val = None
        if dtype in ("integer", "float"):
            # Use errors="coerce" so any non-numeric stragglers become NaN
            # rather than raising an exception.
            numeric = pd.to_numeric(series, errors="coerce").dropna()
            if len(numeric) > 0:
                min_val = float(numeric.min())
                max_val = float(numeric.max())
                mean_val = round(float(numeric.mean()), 4)

        columns.append(
            ColumnSchema(
                name=col,
                dtype=dtype,
                nullable=null_count > 0,
                null_count=null_count,
                null_pct=null_pct,
                unique_count=unique_count,
                sample_values=sample_values,
                min_value=min_val,
                max_value=max_val,
                mean_value=mean_val,
            )
        )

    return DataSchema(
        file=str(Path(file_path).resolve()),
        row_count=len(df),
        column_count=len(df.columns),
        columns=columns,
        captured_at=datetime.now(timezone.utc).isoformat(),
    )


def generate_checks(schema: DataSchema) -> QualityChecks:
    """
    Auto-generate quality rules from a baseline DataSchema.

    Rules are derived directly from what was observed at init time, making
    Pipedog zero-config. The generated rules are intentionally conservative:
    they catch real regressions (nulls appearing in a previously clean column,
    values falling outside the observed range) without being so strict that
    they flag normal day-to-day variation.

    Rules generated per column:

    not_null (if baseline had zero nulls):
        The column must remain fully populated. Any null introduced after
        init will fail this check with exit code 1.

    null_rate (if baseline already had some nulls):
        The null percentage must stay below baseline_pct + 10 percentage
        points. Gives headroom for normal variation while catching spikes.

    min_value / max_value (numeric columns only):
        Values must stay within the range observed at init time. Useful for
        catching upstream bugs (negative IDs, impossible ages, etc.).

    unique (if every value was distinct at init time):
        Column is treated as a key column and must remain duplicate-free.
        A single duplicate will fail this check.

    Args:
        schema: The baseline DataSchema produced by profile_dataframe().

    Returns:
        A QualityChecks object containing all generated rules.
    """
    checks: list[QualityCheck] = []

    for col in schema.columns:
        # --- Nullability ---
        if not col.nullable:
            # Column was null-free at init → enforce strict not-null.
            checks.append(
                QualityCheck(
                    column=col.name,
                    check_type="not_null",
                    description=f"'{col.name}' must have no null values",
                    threshold=0.0,
                )
            )
        elif col.null_pct > 0:
            # Column already had nulls → allow up to 10pp more before alarming.
            checks.append(
                QualityCheck(
                    column=col.name,
                    check_type="null_rate",
                    description=f"'{col.name}' null rate should stay below {min(col.null_pct + 10, 100):.1f}%",
                    threshold=round(min(col.null_pct + 10, 100), 2),
                )
            )

        # --- Numeric range ---
        if col.dtype in ("integer", "float") and col.min_value is not None:
            checks.append(
                QualityCheck(
                    column=col.name,
                    check_type="min_value",
                    description=f"'{col.name}' minimum value should be >= {col.min_value}",
                    threshold=col.min_value,
                )
            )
            checks.append(
                QualityCheck(
                    column=col.name,
                    check_type="max_value",
                    description=f"'{col.name}' maximum value should be <= {col.max_value}",
                    threshold=col.max_value,
                )
            )

        # --- Uniqueness ---
        # Only flag columns where *every* row was unique and the dataset had
        # more than 1 row (a 1-row file trivially has all unique values).
        if col.unique_count == schema.row_count and schema.row_count > 1:
            checks.append(
                QualityCheck(
                    column=col.name,
                    check_type="unique",
                    description=f"'{col.name}' should contain only unique values (looks like a key column)",
                )
            )

    return QualityChecks(
        file=schema.file,
        checks=checks,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


def save_snapshot(schema: DataSchema, checks: QualityChecks) -> None:
    """
    Persist the schema snapshot and quality checks to .pipedog/.

    Creates the .pipedog/ directory if it does not exist, then writes:
        .pipedog/schema.json  — the DataSchema as pretty-printed JSON
        .pipedog/checks.json  — the QualityChecks as pretty-printed JSON

    Overwrites any existing files, so re-running `pipedog init` always
    refreshes the baseline to the current state of the file.

    Args:
        schema: The DataSchema returned by profile_dataframe().
        checks: The QualityChecks returned by generate_checks().
    """
    pipedog_dir = _pipedog_dir()
    pipedog_dir.mkdir(exist_ok=True)

    schema_path = pipedog_dir / SCHEMA_FILE
    schema_path.write_text(schema.model_dump_json(indent=2))

    checks_path = pipedog_dir / CHECKS_FILE
    checks_path.write_text(checks.model_dump_json(indent=2))


def load_snapshot() -> tuple[DataSchema, QualityChecks]:
    """
    Load the baseline schema and quality checks from .pipedog/.

    Reads .pipedog/schema.json and .pipedog/checks.json, validates the JSON
    against the Pydantic models, and returns both objects. Called by
    `pipedog scan` before comparing the new file against the baseline.

    Returns:
        A tuple of (DataSchema, QualityChecks).

    Raises:
        FileNotFoundError: If either snapshot file is missing, with a
                           message directing the user to run `pipedog init`.
    """
    pipedog_dir = _pipedog_dir()
    schema_path = pipedog_dir / SCHEMA_FILE
    checks_path = pipedog_dir / CHECKS_FILE

    if not schema_path.exists():
        raise FileNotFoundError(
            "No schema snapshot found. Run `pipedog init <file>` first."
        )
    if not checks_path.exists():
        raise FileNotFoundError(
            "No quality checks found. Run `pipedog init <file>` first."
        )

    schema = DataSchema.model_validate_json(schema_path.read_text())
    checks = QualityChecks.model_validate_json(checks_path.read_text())
    return schema, checks
