"""
profiler.py — File loading, type inference, statistical profiling, and snapshot I/O.

This module is the core of `pipedog init`. Responsibilities:
  1. Reading CSV, Parquet, JSON, Excel (.xlsx), and Excel Binary (.xlsb) files into a pandas DataFrame.
  2. Inferring a human-readable type for every column.
  3. Computing per-column statistics (nulls, ranges, distribution, allowed values).
  4. Auto-generating quality rules from those statistics.
  5. Merging statistics from multiple files into a single baseline (multi-file init).
  6. Persisting snapshots to / reading snapshots from .pipedog/<profile>/.

Snapshot layout on disk:
    .pipedog/<profile>/
        schema.json   — DataSchema (row count, column stats, timestamp)
        checks.json   — QualityChecks (auto-generated + custom rules)
        history.json  — ScanHistory (appended by history.py after each scan)
        reports/      — HTML scan reports (created by reporter.py)
"""

from __future__ import annotations

import json
import math
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from .schema import ColumnSchema, DataSchema, QualityCheck, QualityChecks

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PIPEDOG_DIR = ".pipedog"
SCHEMA_FILE = "schema.json"
CHECKS_FILE = "checks.json"

# Maximum number of distinct values a string/boolean column can have before
# we stop tracking its allowed-value set (too many values = not categorical).
ALLOWED_VALUES_MAX_CARDINALITY = 50


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def _pipedog_dir(profile: Optional[str] = None) -> Path:
    """
    Return the Path for the snapshot directory for the given profile.

    Profile routing:
        None / "default"  →  Path(".pipedog")           (backward compatible)
        "purchase"        →  Path(".pipedog/purchase")
        "gstr1"           →  Path(".pipedog/gstr1")

    Args:
        profile: Profile name, or None for the default profile.

    Returns:
        A Path object pointing to the profile's snapshot directory.
    """
    base = Path(PIPEDOG_DIR)
    if profile and profile != "default":
        return base / profile
    return base


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------

def get_sheet_names(file_path: str) -> list[str]:
    """Return the list of sheet names for an Excel file (.xlsx or .xlsb)."""
    path = Path(file_path)
    ext = path.suffix.lower()
    if ext == ".xlsx":
        import openpyxl
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        names = wb.sheetnames
        wb.close()
        return names
    elif ext == ".xlsb":
        import pyxlsb
        with pyxlsb.open_workbook(file_path) as wb:
            return wb.sheets
    raise ValueError(f"get_sheet_names only supports .xlsx and .xlsb, got '{ext}'")


def load_file(
    file_path: str,
    sheet_name: str | None = None,
    skiprows: int = 0,
) -> pd.DataFrame:
    """
    Read a data file into a pandas DataFrame.

    Dispatches to the correct pandas reader based on file extension.

    Supported extensions:
        .csv               - read_csv (infers delimiter and dtypes automatically)
        .parquet / .pq     - read_parquet (requires pyarrow)
        .json              - read_json (expects array or records orientation)
        .xlsx              - read_excel via openpyxl
        .xlsb              - read_excel via pyxlsb

    For Excel files, ``sheet_name`` selects which sheet to read. When omitted
    the first sheet is used.

    ``skiprows`` skips that many rows before reading the header. Use this when
    the data file has titles or blank lines above the column header row.

    Args:
        file_path:  Relative or absolute path to the data file.
        sheet_name: Sheet name to read (Excel files only). Defaults to first sheet.
        skiprows:   Number of rows to skip before the header (default 0).

    Returns:
        A pandas DataFrame.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError:        If the file extension is not supported, or if a
                           required Excel engine is not installed.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: '{file_path}'")
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(file_path, skiprows=skiprows or None)
    elif ext in (".parquet", ".pq"):
        return pd.read_parquet(file_path)
    elif ext == ".json":
        return pd.read_json(file_path)
    elif ext == ".xlsx":
        try:
            return pd.read_excel(
                file_path, engine="openpyxl",
                sheet_name=sheet_name or 0,
                skiprows=skiprows or None,
            )
        except ImportError:
            raise ValueError(
                "Reading .xlsx files requires openpyxl. "
                "Install it with: pip install openpyxl"
            )
    elif ext == ".xlsb":
        try:
            return pd.read_excel(
                file_path, engine="pyxlsb",
                sheet_name=sheet_name or 0,
                skiprows=skiprows or None,
            )
        except ImportError:
            raise ValueError(
                "Reading .xlsb files requires pyxlsb. "
                "Install it with: pip install pyxlsb"
            )
    else:
        raise ValueError(
            f"Unsupported file type: '{ext}'. "
            "Supported: .csv, .parquet, .pq, .json, .xlsx, .xlsb"
        )


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------

def _dtype_name(series: pd.Series) -> str:
    """
    Map a pandas Series to a human-readable Pipedog type string.

    Type mapping:
        int* dtype                     → "integer"
        float* dtype                   → "float"
        bool dtype                     → "boolean"
        object dtype + parseable dates → "datetime"
        object dtype + other strings   → "string"
        datetime64[*] dtype            → "datetime"
        anything else                  → raw dtype string (fallback)

    For object columns, heuristically probes the first 5 non-null values
    for datetime parseability. Warnings are suppressed to keep output clean.

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
    return dtype


# ---------------------------------------------------------------------------
# Profiling
# ---------------------------------------------------------------------------

def profile_dataframe(df: pd.DataFrame, file_path: str) -> DataSchema:
    """
    Compute per-column statistics for a DataFrame and return a DataSchema.

    For each column computes:
      - Inferred Pipedog type (via _dtype_name).
      - Null count and null percentage.
      - Distinct value count (excluding nulls).
      - Up to 5 non-null sample values (JSON-safe).
      - Numeric: min, max, mean, std_dev, p25, p50, p75.
      - String/boolean with <= 50 unique values: allowed_values list.

    Also sets DataSchema.row_count_mean / row_count_std / source_files for
    single-file init so the row_count quality check always has a baseline.

    Args:
        df:        The DataFrame to profile.
        file_path: Original path to the source file (stored as absolute path).

    Returns:
        A DataSchema containing file metadata and per-column statistics.
    """
    columns: list[ColumnSchema] = []

    for col in df.columns:
        series = df[col]
        total = len(series)

        null_count = int(series.isna().sum())
        null_pct = round(null_count / total * 100, 2) if total > 0 else 0.0

        # Normalize columns that contain unhashable types (dicts, lists from
        # nested JSON objects/arrays) to their JSON string representation so
        # that nunique(), unique(), and set operations work without error.
        try:
            series.nunique(dropna=True)
        except TypeError:
            series = series.apply(
                lambda x: json.dumps(x, sort_keys=True, default=str)
                if isinstance(x, (dict, list)) else x
            )

        unique_count = int(series.nunique(dropna=True))
        dtype = _dtype_name(series)

        # Sample values — convert non-JSON-serialisable types to strings.
        # Datetime values are trimmed to date-only (YYYY-MM-DD) to avoid
        # column truncation in the terminal profile table.
        sample_raw = series.dropna().head(5).tolist()
        sample_values: list[Any] = []
        for v in sample_raw:
            try:
                json.dumps(v)
                sample_values.append(v)
            except (TypeError, ValueError):
                s = str(v)
                # Trim pandas Timestamp strings (e.g. "2024-01-15 00:00:00") to date only
                if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                    s = s[:10]
                sample_values.append(s)

        # Numeric statistics.
        min_val = max_val = mean_val = std_val = p25 = p50 = p75 = None
        if dtype in ("integer", "float"):
            numeric = pd.to_numeric(series, errors="coerce").dropna()
            if len(numeric) > 0:
                min_val = float(numeric.min())
                max_val = float(numeric.max())
                mean_val = round(float(numeric.mean()), 4)
                std_val = round(float(numeric.std()), 4) if len(numeric) > 1 else 0.0
                p25 = round(float(numeric.quantile(0.25)), 4)
                p50 = round(float(numeric.quantile(0.50)), 4)
                p75 = round(float(numeric.quantile(0.75)), 4)

        # Allowed values — only for low-cardinality string/boolean columns.
        # This enables the allowed_values quality check which catches new
        # category values introduced after the baseline was created.
        allowed_values = None
        if dtype in ("string", "boolean") and 0 < unique_count <= ALLOWED_VALUES_MAX_CARDINALITY:
            allowed_values = sorted(
                [str(v) for v in series.dropna().unique().tolist()]
            )

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
                std_dev=std_val,
                p25=p25,
                p50=p50,
                p75=p75,
                allowed_values=allowed_values,
            )
        )

    abs_path = str(Path(file_path).resolve())
    row_count = len(df)

    return DataSchema(
        file=abs_path,
        row_count=row_count,
        column_count=len(df.columns),
        columns=columns,
        captured_at=datetime.now(timezone.utc).isoformat(),
        # Single-file baselines still populate these so row_count check fires.
        source_files=[abs_path],
        row_count_mean=float(row_count),
        row_count_std=0.0,
    )


# ---------------------------------------------------------------------------
# Multi-file merge
# ---------------------------------------------------------------------------

def merge_schemas(schemas: list[DataSchema], file_paths: list[str]) -> DataSchema:
    """
    Merge per-file DataSchema objects into one representative baseline.

    Used by `pipedog init file1.csv file2.csv file3.csv` to create a single
    baseline that reflects the normal pattern across all input files.

    Column consistency rules:
        - Every file must have the same column names in the same order.
        - Every file must have the same dtype for each column.
        Raises ValueError with a clear message if either rule is violated.

    Merging rules per field:
        dtype           → must be identical (already validated)
        nullable        → True if ANY file had a null in that column
        null_count      → sum across all files
        null_pct        → weighted average by row count
        unique_count    → set to -1 (cross-file uniqueness is undefined)
        sample_values   → union of all sample_values, up to 5 entries
        min_value       → global minimum across all files
        max_value       → global maximum across all files
        mean_value      → weighted average by row count
        std_dev         → None (cannot reconstruct from per-file std devs)
        p25/p50/p75     → simple average of per-file percentiles
        allowed_values  → union of all per-file sets; set to None if union > 50

    DataSchema-level merging:
        row_count       → total rows across all files
        row_count_mean  → mean of per-file row counts
        row_count_std   → std dev of per-file row counts
        source_files    → all input file paths
        file            → the first file path (representative label)

    Args:
        schemas:    List of DataSchema objects (one per input file).
        file_paths: Corresponding original file path strings.

    Returns:
        A single merged DataSchema representing the baseline.

    Raises:
        ValueError: If column structures differ across input files.
    """
    if len(schemas) == 1:
        return schemas[0]

    ref = schemas[0]

    # --- Validate structural consistency across all files ---
    for i, s in enumerate(schemas[1:], start=2):
        ref_names = [c.name for c in ref.columns]
        cur_names = [c.name for c in s.columns]
        if ref_names != cur_names:
            raise ValueError(
                f"Column mismatch between file 1 and file {i}.\n"
                f"  File 1 columns: {ref_names}\n"
                f"  File {i} columns: {cur_names}\n"
                "All files must have identical column names in the same order."
            )
        for r_col, c_col in zip(ref.columns, s.columns):
            if r_col.dtype != c_col.dtype:
                raise ValueError(
                    f"Type mismatch in column '{r_col.name}': "
                    f"file 1 has '{r_col.dtype}', file {i} has '{c_col.dtype}'.\n"
                    "All files must have the same column types."
                )

    # --- Merge per-column statistics ---
    row_counts = [s.row_count for s in schemas]
    total_rows = sum(row_counts)
    merged_columns: list[ColumnSchema] = []

    for col_idx, ref_col in enumerate(ref.columns):
        col_name = ref_col.name
        dtype = ref_col.dtype

        all_cols = [s.columns[col_idx] for s in schemas]

        nullable = any(c.nullable for c in all_cols)
        null_count = sum(c.null_count for c in all_cols)
        # Weighted average null percentage.
        null_pct = round(
            sum(c.null_pct * r for c, r in zip(all_cols, row_counts)) / total_rows, 2
        ) if total_rows > 0 else 0.0

        # Union of sample values, deduplicated, up to 5.
        seen: list[Any] = []
        for c in all_cols:
            for v in c.sample_values:
                if v not in seen:
                    seen.append(v)
                if len(seen) >= 5:
                    break
            if len(seen) >= 5:
                break
        sample_values = seen

        # Numeric stats.
        min_val = max_val = mean_val = p25 = p50 = p75 = None
        if dtype in ("integer", "float"):
            mins = [c.min_value for c in all_cols if c.min_value is not None]
            maxs = [c.max_value for c in all_cols if c.max_value is not None]
            means = [c.mean_value for c in all_cols if c.mean_value is not None]
            p25s = [c.p25 for c in all_cols if c.p25 is not None]
            p50s = [c.p50 for c in all_cols if c.p50 is not None]
            p75s = [c.p75 for c in all_cols if c.p75 is not None]
            if mins:
                min_val = min(mins)
            if maxs:
                max_val = max(maxs)
            if means:
                # Weighted average mean.
                non_null_counts = [r - c.null_count for c, r in zip(all_cols, row_counts)]
                total_non_null = sum(non_null_counts)
                if total_non_null > 0:
                    mean_val = round(
                        sum(m * n for m, n in zip(means, non_null_counts)) / total_non_null, 4
                    )
            if p25s:
                p25 = round(sum(p25s) / len(p25s), 4)
            if p50s:
                p50 = round(sum(p50s) / len(p50s), 4)
            if p75s:
                p75 = round(sum(p75s) / len(p75s), 4)

        # Allowed values: union of all sets; discard if union > 50.
        allowed_values = None
        if dtype in ("string", "boolean"):
            all_sets = [set(c.allowed_values) for c in all_cols if c.allowed_values is not None]
            if all_sets:
                union_vals = set().union(*all_sets)
                if len(union_vals) <= ALLOWED_VALUES_MAX_CARDINALITY:
                    allowed_values = sorted(list(union_vals))

        # Average std dev across files as a representative baseline.
        std_devs = [c.std_dev for c in all_cols if c.std_dev is not None]
        merged_std_dev = round(sum(std_devs) / len(std_devs), 4) if std_devs else None

        # Track whether the column was a key column (all unique) in every file.
        all_unique_flag = all(
            c.unique_count == row_counts[i]
            for i, c in enumerate(all_cols)
            if c.unique_count != -1
        )

        merged_columns.append(
            ColumnSchema(
                name=col_name,
                dtype=dtype,
                nullable=nullable,
                null_count=null_count,
                null_pct=null_pct,
                unique_count=-1,  # Undefined across multiple files; use all_unique instead.
                sample_values=sample_values,
                min_value=min_val,
                max_value=max_val,
                mean_value=mean_val,
                std_dev=merged_std_dev,
                p25=p25,
                p50=p50,
                p75=p75,
                allowed_values=allowed_values,
                all_unique=all_unique_flag,
            )
        )

    # --- Row count statistics ---
    row_count_mean = sum(row_counts) / len(row_counts)
    row_count_std = (
        math.sqrt(sum((r - row_count_mean) ** 2 for r in row_counts) / len(row_counts))
        if len(row_counts) > 1 else 0.0
    )

    return DataSchema(
        file=str(Path(file_paths[0]).resolve()),
        row_count=total_rows,
        column_count=ref.column_count,
        columns=merged_columns,
        captured_at=datetime.now(timezone.utc).isoformat(),
        source_files=[str(Path(p).resolve()) for p in file_paths],
        row_count_mean=round(row_count_mean, 2),
        row_count_std=round(row_count_std, 2),
    )


# ---------------------------------------------------------------------------
# Check generation
# ---------------------------------------------------------------------------

def generate_checks(schema: DataSchema) -> QualityChecks:
    """
    Auto-generate quality rules from a baseline DataSchema.

    Rules are derived directly from what was observed at init time, making
    Pipedog zero-config. Generated rules per column:

        not_null        If baseline had zero nulls → enforce strictly.
        null_rate       If baseline had some nulls → allow up to pct + 10pp.
        min_value       Numeric: lock in observed minimum.
        max_value       Numeric: lock in observed maximum.
        unique          Column was fully unique (key column detection).
        allowed_values  Low-cardinality string/bool: flag new category values.
        std_dev_change  Numeric: flag if std deviation changes > 50%.

    Schema-level rules (one per file, not per column):
        row_count       File must have >= 80% of baseline average row count.

    Args:
        schema: The baseline DataSchema produced by profile_dataframe()
                or merge_schemas().

    Returns:
        A QualityChecks object containing all generated rules.
    """
    checks: list[QualityCheck] = []

    for col in schema.columns:
        # --- Nullability ---
        if not col.nullable:
            checks.append(QualityCheck(
                column=col.name,
                check_type="not_null",
                description=f"'{col.name}' must have no null values",
                threshold=0.0,
            ))
        elif col.null_pct > 0:
            checks.append(QualityCheck(
                column=col.name,
                check_type="null_rate",
                description=(
                    f"'{col.name}' null rate should stay below "
                    f"{min(col.null_pct + 10, 100):.1f}%"
                ),
                threshold=round(min(col.null_pct + 10, 100), 2),
            ))

        # --- Numeric range ---
        if col.dtype in ("integer", "float") and col.min_value is not None:
            checks.append(QualityCheck(
                column=col.name,
                check_type="min_value",
                description=f"'{col.name}' minimum value should be >= {col.min_value}",
                threshold=col.min_value,
            ))
            checks.append(QualityCheck(
                column=col.name,
                check_type="max_value",
                description=f"'{col.name}' maximum value should be <= {col.max_value}",
                threshold=col.max_value,
            ))

        # --- Uniqueness (key column detection) ---
        # For single-file baselines: unique_count == row_count.
        # For merged baselines: unique_count == -1, so fall back to all_unique flag.
        is_key_col = (
            (col.unique_count == schema.row_count and schema.row_count > 1)
            or col.all_unique is True
        )
        if is_key_col:
            checks.append(QualityCheck(
                column=col.name,
                check_type="unique",
                description=(
                    f"'{col.name}' should contain only unique values "
                    "(looks like a key column)"
                ),
            ))

        # --- Allowed values (new category detection) ---
        if col.allowed_values is not None:
            checks.append(QualityCheck(
                column=col.name,
                check_type="allowed_values",
                description=(
                    f"'{col.name}' must only contain "
                    f"{len(col.allowed_values)} known values"
                ),
                expected_value=col.allowed_values,
            ))

        # --- Distribution drift ---
        if col.std_dev is not None and col.std_dev > 0:
            checks.append(QualityCheck(
                column=col.name,
                check_type="std_dev_change",
                description=(
                    f"'{col.name}' std deviation should not change "
                    f"> 50% from baseline ({col.std_dev})"
                ),
                threshold=col.std_dev,
            ))

    # --- Row count check (schema-level, not per-column) ---
    # Uses row_count_mean so multi-file baselines get a sensible threshold.
    baseline_mean = schema.row_count_mean or float(schema.row_count)
    threshold_rows = baseline_mean * 0.80
    checks.append(QualityCheck(
        column="__row_count__",
        check_type="row_count",
        description=(
            f"File must have at least 80% of baseline row count "
            f"({baseline_mean:.0f} rows avg)"
        ),
        threshold=round(threshold_rows, 2),
    ))

    return QualityChecks(
        file=schema.file,
        checks=checks,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )


# ---------------------------------------------------------------------------
# Snapshot I/O
# ---------------------------------------------------------------------------

def save_snapshot(
    schema: DataSchema,
    checks: QualityChecks,
    profile: Optional[str] = None,
) -> None:
    """
    Persist the schema snapshot and quality checks to .pipedog/<profile>/.

    Creates the directory tree if it does not exist (including the reports/
    subdirectory), then writes schema.json and checks.json. Overwrites any
    existing files, so re-running `pipedog init` always refreshes the baseline.

    Args:
        schema:  The DataSchema returned by profile_dataframe() or merge_schemas().
        checks:  The QualityChecks returned by generate_checks().
        profile: Profile name; None for the default profile.
    """
    pipedog_dir = _pipedog_dir(profile)
    pipedog_dir.mkdir(parents=True, exist_ok=True)
    # Pre-create reports/ so the first scan can write there immediately.
    (pipedog_dir / "reports").mkdir(exist_ok=True)

    (pipedog_dir / SCHEMA_FILE).write_text(schema.model_dump_json(indent=2))
    (pipedog_dir / CHECKS_FILE).write_text(checks.model_dump_json(indent=2))


def load_snapshot(
    profile: Optional[str] = None,
) -> tuple[DataSchema, QualityChecks]:
    """
    Load the baseline schema and quality checks from .pipedog/<profile>/.

    Reads schema.json and checks.json, validates against Pydantic models,
    and returns both objects. Called by `pipedog scan` before comparing
    the new file against the baseline.

    Args:
        profile: Profile name; None for the default profile.

    Returns:
        A tuple of (DataSchema, QualityChecks).

    Raises:
        FileNotFoundError: If either snapshot file is missing, with a message
                           directing the user to run `pipedog init`.
    """
    pipedog_dir = _pipedog_dir(profile)
    schema_path = pipedog_dir / SCHEMA_FILE
    checks_path = pipedog_dir / CHECKS_FILE
    profile_hint = f" --profile {profile}" if profile else ""

    if not schema_path.exists():
        raise FileNotFoundError(
            f"No schema snapshot found. "
            f"Run `pipedog init <file>{profile_hint}` first."
        )
    if not checks_path.exists():
        raise FileNotFoundError(
            f"No quality checks found. "
            f"Run `pipedog init <file>{profile_hint}` first."
        )

    schema = DataSchema.model_validate_json(schema_path.read_text())
    checks = QualityChecks.model_validate_json(checks_path.read_text())
    return schema, checks
