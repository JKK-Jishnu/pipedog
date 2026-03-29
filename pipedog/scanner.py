"""
scanner.py — Schema drift detection and quality check evaluation.

This module powers `pipedog scan`. It contains two independent concerns:

1. Structural drift detection (detect_drift):
   Compares the column structure of the current file against the baseline
   snapshot. Reports added columns (warning), removed columns (error), and
   type changes (error). Does NOT evaluate numeric thresholds — that is
   handled by run_quality_checks.

2. Quality check evaluation (run_quality_checks):
   Iterates over the auto-generated rules in .pipedog/checks.json and
   evaluates each one against the current file's statistics. Returns a
   CheckResult for every rule, regardless of whether it passed or failed.

Neither function reads from disk or writes to disk — all I/O is handled by
profiler.py (loading the snapshot) and output.py (rendering results).
"""

from __future__ import annotations

import pandas as pd

from .profiler import profile_dataframe
from .schema import CheckResult, ColumnSchema, DataSchema, QualityCheck, QualityChecks


def detect_drift(baseline: DataSchema, current: DataSchema) -> list[CheckResult]:
    """
    Compare two DataSchema objects and return structural drift findings.

    Drift is detected at three levels of granularity:

    1. Added columns (severity: warning)
       A column present in the current file but absent from the baseline.
       Treated as a warning because new columns may be intentional additions
       by upstream teams and do not necessarily indicate a problem.

    2. Removed columns (severity: error)
       A column present in the baseline but absent from the current file.
       Treated as an error because downstream pipelines that depend on that
       column will break.

    3. Type changes (severity: error)
       A column exists in both files but its Pipedog type has changed
       (e.g. "integer" → "string"). This usually means an upstream schema
       change or a data corruption issue.

    Args:
        baseline: The DataSchema loaded from .pipedog/schema.json.
        current:  The DataSchema profiled from the current file.

    Returns:
        A list of CheckResult objects. Empty list means no structural drift.
    """
    results: list[CheckResult] = []

    # Build name → ColumnSchema lookup dicts for O(1) access.
    baseline_cols = {c.name: c for c in baseline.columns}
    current_cols = {c.name: c for c in current.columns}

    # --- Pass 1: detect columns added to the current file ---
    for col_name in current_cols:
        if col_name not in baseline_cols:
            results.append(
                CheckResult(
                    column=col_name,
                    check_type="schema_drift",
                    description=f"New column '{col_name}' was added",
                    passed=False,
                    detail=f"Column '{col_name}' ({current_cols[col_name].dtype}) did not exist in the baseline snapshot.",
                    severity="warning",  # Additions are warnings, not errors.
                )
            )

    # --- Pass 2: detect columns removed from the current file ---
    for col_name in baseline_cols:
        if col_name not in current_cols:
            results.append(
                CheckResult(
                    column=col_name,
                    check_type="schema_drift",
                    description=f"Column '{col_name}' was removed",
                    passed=False,
                    detail=f"Column '{col_name}' existed in the baseline but is missing from the current file.",
                    severity="error",  # Removals break downstream consumers.
                )
            )

    # --- Pass 3: detect type changes for columns present in both ---
    for col_name in baseline_cols:
        if col_name not in current_cols:
            continue  # Already reported as removed above.
        b_col = baseline_cols[col_name]
        c_col = current_cols[col_name]
        if b_col.dtype != c_col.dtype:
            results.append(
                CheckResult(
                    column=col_name,
                    check_type="type_change",
                    description=f"Column '{col_name}' changed type from {b_col.dtype} to {c_col.dtype}",
                    passed=False,
                    detail=(
                        f"'{col_name}' was '{b_col.dtype}' in the baseline "
                        f"but is now '{c_col.dtype}'."
                    ),
                    severity="error",
                )
            )

    return results


def run_quality_checks(
    df: pd.DataFrame, current_schema: DataSchema, checks: QualityChecks
) -> list[CheckResult]:
    """
    Evaluate every quality rule against the current file's statistics.

    Iterates over the rules stored in .pipedog/checks.json and compares
    each threshold against the corresponding statistic in current_schema.
    The raw DataFrame (df) is accepted as a parameter for forward
    compatibility but statistics are read from current_schema to avoid
    re-computing them.

    Check evaluation logic:

        not_null:   Passes if current null_count == 0.
        null_rate:  Passes if current null_pct <= check.threshold.
        min_value:  Passes if current min_value >= check.threshold.
        max_value:  Passes if current max_value <= check.threshold.
        unique:     Passes if current unique_count == current row count.

    Columns that were removed (drift) are silently skipped here because
    detect_drift() already reports them. This avoids duplicate error messages.

    Args:
        df:             The current DataFrame (used for row count context).
        current_schema: Statistics profiled from the current file.
        checks:         The QualityChecks loaded from .pipedog/checks.json.

    Returns:
        A list of CheckResult objects, one per evaluated rule. Rules for
        missing columns are omitted entirely.
    """
    results: list[CheckResult] = []

    # Build a name → ColumnSchema lookup for fast access inside the loop.
    col_map = {c.name: c for c in current_schema.columns}

    for check in checks.checks:
        col_name = check.column

        # If the column was removed, drift detection already reported it.
        # Skip to avoid a duplicate (and confusing) error message.
        if col_name not in col_map:
            continue

        col = col_map[col_name]

        # ------------------------------------------------------------------
        # not_null — column must be completely free of nulls.
        # Severity: error (exit code 1).
        # ------------------------------------------------------------------
        if check.check_type == "not_null":
            passed = col.null_count == 0
            results.append(
                CheckResult(
                    column=col_name,
                    check_type="not_null",
                    description=check.description,
                    passed=passed,
                    detail=(
                        f"No nulls found in '{col_name}'."
                        if passed
                        else f"'{col_name}' has {col.null_count} null value(s) ({col.null_pct}% of rows)."
                    ),
                )
            )

        # ------------------------------------------------------------------
        # null_rate — null percentage must stay below the stored threshold.
        # Severity: warning (scan still passes overall).
        # ------------------------------------------------------------------
        elif check.check_type == "null_rate":
            threshold = check.threshold or 100.0
            passed = col.null_pct <= threshold
            results.append(
                CheckResult(
                    column=col_name,
                    check_type="null_rate",
                    description=check.description,
                    passed=passed,
                    detail=(
                        f"'{col_name}' null rate is {col.null_pct}%, within threshold of {threshold}%."
                        if passed
                        else f"'{col_name}' null rate is {col.null_pct}%, exceeding threshold of {threshold}%."
                    ),
                    severity="warning",
                )
            )

        # ------------------------------------------------------------------
        # min_value — numeric minimum must be >= the baseline minimum.
        # Severity: error.
        # ------------------------------------------------------------------
        elif check.check_type == "min_value":
            if col.min_value is not None and check.threshold is not None:
                passed = col.min_value >= check.threshold
                results.append(
                    CheckResult(
                        column=col_name,
                        check_type="min_value",
                        description=check.description,
                        passed=passed,
                        detail=(
                            f"'{col_name}' minimum is {col.min_value}, meets baseline minimum of {check.threshold}."
                            if passed
                            else f"'{col_name}' minimum is {col.min_value}, below baseline minimum of {check.threshold}."
                        ),
                    )
                )

        # ------------------------------------------------------------------
        # max_value — numeric maximum must be <= the baseline maximum.
        # Severity: error.
        # ------------------------------------------------------------------
        elif check.check_type == "max_value":
            if col.max_value is not None and check.threshold is not None:
                passed = col.max_value <= check.threshold
                results.append(
                    CheckResult(
                        column=col_name,
                        check_type="max_value",
                        description=check.description,
                        passed=passed,
                        detail=(
                            f"'{col_name}' maximum is {col.max_value}, within baseline maximum of {check.threshold}."
                            if passed
                            else f"'{col_name}' maximum is {col.max_value}, exceeds baseline maximum of {check.threshold}."
                        ),
                    )
                )

        # ------------------------------------------------------------------
        # unique — every value in the column must be distinct.
        # Severity: error. Triggered when unique_count < row_count.
        # ------------------------------------------------------------------
        elif check.check_type == "unique":
            is_unique = col.unique_count == current_schema.row_count
            passed = is_unique
            results.append(
                CheckResult(
                    column=col_name,
                    check_type="unique",
                    description=check.description,
                    passed=passed,
                    detail=(
                        f"'{col_name}' has {col.unique_count} unique values across {current_schema.row_count} rows — all unique."
                        if passed
                        else f"'{col_name}' has {col.unique_count} unique values but {current_schema.row_count} rows — duplicates detected."
                    ),
                )
            )

    return results
