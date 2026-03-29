"""
scanner.py — Schema drift detection and quality check evaluation.

This module powers `pipedog scan`. Two independent concerns:

1. detect_drift()
   Compares column structure of the current file against the baseline snapshot.
   Reports added columns (warning), removed columns (error), and type changes
   (error). Does NOT evaluate numeric thresholds — that is handled below.

2. run_quality_checks()
   Iterates over the rules in .pipedog/<profile>/checks.json and evaluates
   each one against the current file's statistics. Supported check types:

       not_null        — column must have zero nulls
       null_rate       — null % must stay below threshold
       min_value       — numeric minimum must be >= threshold
       max_value       — numeric maximum must be <= threshold
       unique          — every value must be distinct
       row_count       — file row count must be >= threshold (80% of baseline avg)
       allowed_values  — column must only contain values seen at init time
       std_dev_change  — std deviation must not change > 50% from baseline

Neither function reads from or writes to disk — all I/O is handled by
profiler.py (snapshot loading) and history.py (result persistence).
"""

from __future__ import annotations

import pandas as pd

from .schema import CheckResult, DataSchema, QualityChecks


def detect_drift(baseline: DataSchema, current: DataSchema) -> list[CheckResult]:
    """
    Compare two DataSchema objects and return structural drift findings.

    Three levels of drift detected:

    1. Added columns (severity: warning)
       Present in current but absent from baseline. Treated as a warning
       because new columns may be intentional upstream additions.

    2. Removed columns (severity: error)
       Present in baseline but absent from current. Error because downstream
       consumers that depend on the column will break.

    3. Type changes (severity: error)
       Column exists in both files but Pipedog type has changed
       (e.g. "integer" → "string"). Usually indicates an upstream schema
       change or data corruption.

    Args:
        baseline: The DataSchema loaded from .pipedog/<profile>/schema.json.
        current:  The DataSchema profiled from the current file.

    Returns:
        A list of CheckResult objects. Empty list means no structural drift.
    """
    results: list[CheckResult] = []

    baseline_cols = {c.name: c for c in baseline.columns}
    current_cols = {c.name: c for c in current.columns}

    # --- Pass 1: columns added to the current file ---
    for col_name in current_cols:
        if col_name not in baseline_cols:
            results.append(CheckResult(
                column=col_name,
                check_type="schema_drift",
                description=f"New column '{col_name}' was added",
                passed=False,
                detail=(
                    f"Column '{col_name}' ({current_cols[col_name].dtype}) "
                    "did not exist in the baseline snapshot."
                ),
                severity="warning",
            ))

    # --- Pass 2: columns removed from the current file ---
    for col_name in baseline_cols:
        if col_name not in current_cols:
            results.append(CheckResult(
                column=col_name,
                check_type="schema_drift",
                description=f"Column '{col_name}' was removed",
                passed=False,
                detail=(
                    f"Column '{col_name}' existed in the baseline "
                    "but is missing from the current file."
                ),
                severity="error",
            ))

    # --- Pass 3: type changes for columns present in both ---
    for col_name in baseline_cols:
        if col_name not in current_cols:
            continue  # Already reported as removed above.
        b_col = baseline_cols[col_name]
        c_col = current_cols[col_name]
        if b_col.dtype != c_col.dtype:
            results.append(CheckResult(
                column=col_name,
                check_type="type_change",
                description=(
                    f"Column '{col_name}' changed type "
                    f"from {b_col.dtype} to {c_col.dtype}"
                ),
                passed=False,
                detail=(
                    f"'{col_name}' was '{b_col.dtype}' in the baseline "
                    f"but is now '{c_col.dtype}'."
                ),
                severity="error",
            ))

    return results


def run_quality_checks(
    df: pd.DataFrame,
    current_schema: DataSchema,
    checks: QualityChecks,
) -> list[CheckResult]:
    """
    Evaluate every quality rule against the current file's statistics.

    Iterates over the rules stored in checks.json and compares each threshold
    against the corresponding statistic in current_schema. The raw DataFrame
    (df) is used for the allowed_values check (requires access to raw values).

    Columns that were removed (drift) are silently skipped here because
    detect_drift() already reports them — avoids duplicate error messages.

    Check evaluation:
        not_null:       passes if current null_count == 0
        null_rate:      passes if current null_pct <= threshold
        min_value:      passes if current min_value >= threshold
        max_value:      passes if current max_value <= threshold
        unique:         passes if current unique_count == current row_count
        row_count:      passes if current row_count >= threshold
        allowed_values: passes if no new values exist beyond the baseline set
        std_dev_change: passes if std dev change is <= 50%

    Args:
        df:             The current DataFrame (needed for allowed_values).
        current_schema: Statistics profiled from the current file.
        checks:         The QualityChecks loaded from checks.json.

    Returns:
        A list of CheckResult objects, one per evaluated rule.
    """
    results: list[CheckResult] = []
    col_map = {c.name: c for c in current_schema.columns}

    for check in checks.checks:
        col_name = check.column

        # Skip rules for columns that no longer exist in the current file.
        # The special sentinel "__row_count__" is a schema-level check, not
        # tied to a real column, so it bypasses this guard.
        if col_name not in col_map and col_name != "__row_count__":
            continue

        col = col_map.get(col_name)

        # ------------------------------------------------------------------
        # not_null — column must be completely free of nulls
        # Severity: error
        # ------------------------------------------------------------------
        if check.check_type == "not_null":
            passed = col.null_count == 0
            results.append(CheckResult(
                column=col_name,
                check_type="not_null",
                description=check.description,
                passed=passed,
                detail=(
                    f"No nulls found in '{col_name}'."
                    if passed else
                    f"'{col_name}' has {col.null_count} null value(s) "
                    f"({col.null_pct}% of rows)."
                ),
            ))

        # ------------------------------------------------------------------
        # null_rate — null percentage must stay below the stored threshold
        # Severity: warning
        # ------------------------------------------------------------------
        elif check.check_type == "null_rate":
            threshold = check.threshold or 100.0
            passed = col.null_pct <= threshold
            results.append(CheckResult(
                column=col_name,
                check_type="null_rate",
                description=check.description,
                passed=passed,
                detail=(
                    f"'{col_name}' null rate is {col.null_pct}%, "
                    f"within threshold of {threshold}%."
                    if passed else
                    f"'{col_name}' null rate is {col.null_pct}%, "
                    f"exceeding threshold of {threshold}%."
                ),
                severity="warning",
            ))

        # ------------------------------------------------------------------
        # min_value — numeric minimum must be >= the baseline minimum
        # Severity: error
        # ------------------------------------------------------------------
        elif check.check_type == "min_value":
            if col.min_value is not None and check.threshold is not None:
                passed = col.min_value >= check.threshold
                results.append(CheckResult(
                    column=col_name,
                    check_type="min_value",
                    description=check.description,
                    passed=passed,
                    detail=(
                        f"'{col_name}' minimum is {col.min_value}, "
                        f"meets baseline minimum of {check.threshold}."
                        if passed else
                        f"'{col_name}' minimum is {col.min_value}, "
                        f"below baseline minimum of {check.threshold}."
                    ),
                ))

        # ------------------------------------------------------------------
        # max_value — numeric maximum must be <= the baseline maximum
        # Severity: error
        # ------------------------------------------------------------------
        elif check.check_type == "max_value":
            if col.max_value is not None and check.threshold is not None:
                passed = col.max_value <= check.threshold
                results.append(CheckResult(
                    column=col_name,
                    check_type="max_value",
                    description=check.description,
                    passed=passed,
                    detail=(
                        f"'{col_name}' maximum is {col.max_value}, "
                        f"within baseline maximum of {check.threshold}."
                        if passed else
                        f"'{col_name}' maximum is {col.max_value}, "
                        f"exceeds baseline maximum of {check.threshold}."
                    ),
                ))

        # ------------------------------------------------------------------
        # unique — every value in the column must be distinct
        # Severity: error
        # ------------------------------------------------------------------
        elif check.check_type == "unique":
            is_unique = col.unique_count == current_schema.row_count
            results.append(CheckResult(
                column=col_name,
                check_type="unique",
                description=check.description,
                passed=is_unique,
                detail=(
                    f"'{col_name}' has {col.unique_count} unique values "
                    f"across {current_schema.row_count} rows — all unique."
                    if is_unique else
                    f"'{col_name}' has {col.unique_count} unique values "
                    f"but {current_schema.row_count} rows — duplicates detected."
                ),
            ))

        # ------------------------------------------------------------------
        # row_count — file must have >= threshold rows (80% of baseline avg)
        # Uses the sentinel column "__row_count__" (not a real column).
        # Severity: error
        # ------------------------------------------------------------------
        elif check.check_type == "row_count":
            actual = current_schema.row_count
            threshold = check.threshold or 0.0
            passed = actual >= threshold
            results.append(CheckResult(
                column="__row_count__",
                check_type="row_count",
                description=check.description,
                passed=passed,
                detail=(
                    f"Row count is {actual:,}, meets minimum of {threshold:,.0f}."
                    if passed else
                    f"Row count is {actual:,}, below minimum of {threshold:,.0f} "
                    f"(80% of baseline average)."
                ),
                severity="error",
            ))

        # ------------------------------------------------------------------
        # allowed_values — column must only contain values seen at init time
        # New category values that were never in the baseline are flagged.
        # Severity: error
        # ------------------------------------------------------------------
        elif check.check_type == "allowed_values":
            if col_name in df.columns and check.expected_value is not None:
                allowed = set(str(v) for v in check.expected_value)
                actual_vals = set(
                    str(v) for v in df[col_name].dropna().unique().tolist()
                )
                new_vals = sorted(actual_vals - allowed)
                passed = len(new_vals) == 0
                results.append(CheckResult(
                    column=col_name,
                    check_type="allowed_values",
                    description=check.description,
                    passed=passed,
                    detail=(
                        f"'{col_name}' contains only known values."
                        if passed else
                        f"'{col_name}' has {len(new_vals)} new value(s) not seen "
                        f"at init time: {new_vals[:5]}"
                        + (" ..." if len(new_vals) > 5 else ".")
                    ),
                ))

        # ------------------------------------------------------------------
        # std_dev_change — std deviation must not change > 50% from baseline
        # Catches silent distribution shifts that min/max alone won't detect.
        # Severity: warning (distribution shifts may be intentional)
        # ------------------------------------------------------------------
        elif check.check_type == "std_dev_change":
            if (
                col is not None
                and col.std_dev is not None
                and check.threshold is not None
                and check.threshold > 0
            ):
                baseline_std = check.threshold
                current_std = col.std_dev
                change_pct = abs(current_std - baseline_std) / baseline_std * 100
                passed = change_pct <= 50.0
                results.append(CheckResult(
                    column=col_name,
                    check_type="std_dev_change",
                    description=check.description,
                    passed=passed,
                    detail=(
                        f"'{col_name}' std deviation changed {change_pct:.1f}% "
                        f"(baseline={baseline_std}, current={current_std})."
                    ),
                    severity="warning",
                ))

    return results
