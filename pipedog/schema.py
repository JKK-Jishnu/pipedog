"""
schema.py — Pydantic data models for Pipedog.

All data flowing between the profiler, scanner, output, reporter, and history
layers is typed using these models. Pydantic validates data at construction
time and handles JSON serialisation/deserialisation for .pipedog/ files.

Models:
    ColumnSchema      — per-column statistics from a single profiling run
    DataSchema        — full file snapshot (collection of ColumnSchema + metadata)
    QualityCheck      — a single auto-generated or custom quality rule
    QualityChecks     — container for all rules for a profile
    CheckResult       — outcome of evaluating one rule during a scan
    ScanHistoryEntry  — one row in the per-profile scan audit log
    ScanHistory       — the full audit log for a profile
"""

from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel


class ColumnSchema(BaseModel):
    """
    Statistics for a single column captured during `pipedog init`.

    These values become the baseline that future `pipedog scan` runs compare
    against. All fields are JSON-serialisable so the snapshot can be stored
    in .pipedog/<profile>/schema.json.

    Attributes:
        name:           Column name as it appears in the file header.
        dtype:          Human-readable type: "integer", "float", "string",
                        "boolean", or "datetime".
        nullable:       True if at least one null/NaN was found at init time.
        null_count:     Absolute number of null rows.
        null_pct:       Null rows as a percentage of total rows (0–100).
        unique_count:   Number of distinct non-null values.
        sample_values:  Up to 5 representative non-null values.
        min_value:      Numeric minimum (None for non-numeric columns).
        max_value:      Numeric maximum (None for non-numeric columns).
        mean_value:     Numeric mean rounded to 4 decimal places.
        std_dev:        Numeric standard deviation rounded to 4 decimal places.
                        Used to detect distribution drift between scans.
        p25:            25th percentile (Q1) for numeric columns.
        p50:            50th percentile (median) for numeric columns.
        p75:            75th percentile (Q3) for numeric columns.
        allowed_values: Sorted list of all distinct values seen at init time,
                        for string/boolean columns with <= 50 unique values.
                        None means the column is not tracked for new values.
    """

    name: str
    dtype: str
    nullable: bool
    null_count: int
    null_pct: float
    unique_count: int
    sample_values: list[Any]
    # Numeric stats — only populated for integer / float columns.
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    # Distribution stats (v0.2.0) — optional for backward compat with old snapshots.
    std_dev: Optional[float] = None
    p25: Optional[float] = None
    p50: Optional[float] = None
    p75: Optional[float] = None
    # Allowed values (v0.2.0) — only for low-cardinality string/boolean columns.
    allowed_values: Optional[list[Any]] = None


class DataSchema(BaseModel):
    """
    Full schema snapshot for a data file, written to .pipedog/<profile>/schema.json.

    Captures both file-level metadata (row/column counts, timestamp) and
    per-column statistics (via ColumnSchema). This is the baseline that
    `pipedog scan` compares new files against.

    Attributes:
        file:           Absolute path to the file at init time.
        row_count:      Total number of rows (sum across all files for multi-file init).
        column_count:   Total number of columns.
        columns:        Ordered list of per-column statistics.
        captured_at:    ISO-8601 UTC timestamp of when the snapshot was taken.
        source_files:   All file paths used to build the baseline (v0.2.0).
                        Single-file init stores a one-element list.
        row_count_mean: Average row count across source files (v0.2.0).
                        Used by the row_count quality check.
        row_count_std:  Standard deviation of row count across source files (v0.2.0).
    """

    file: str
    row_count: int
    column_count: int
    columns: list[ColumnSchema]
    captured_at: str
    # Multi-file baseline fields (v0.2.0) — optional for backward compat.
    source_files: Optional[list[str]] = None
    row_count_mean: Optional[float] = None
    row_count_std: Optional[float] = None


class QualityCheck(BaseModel):
    """
    A single auto-generated or custom quality rule, stored in checks.json.

    Rules are generated from the baseline snapshot by generate_checks() in
    profiler.py and evaluated against new data by run_quality_checks() in
    scanner.py. Users can also add custom rules via `pipedog checks add`.

    Supported check_type values:
        "not_null"       — column must have zero nulls.
        "null_rate"      — null % must stay below threshold.
        "min_value"      — numeric minimum must be >= threshold.
        "max_value"      — numeric maximum must be <= threshold.
        "unique"         — every value must be distinct.
        "row_count"      — file must have >= threshold rows (80% of baseline avg).
        "allowed_values" — column must only contain values in expected_value list.
        "std_dev_change" — std deviation must not change by > 50% from threshold.

    Attributes:
        column:         Name of the column the rule applies to. Use
                        "__row_count__" for the file-level row count check.
        check_type:     One of the type strings listed above.
        description:    Plain-English description shown in the terminal.
        threshold:      Numeric threshold for range / null-rate / row-count checks.
        expected_value: List of allowed values for the allowed_values check type.
    """

    column: str
    check_type: str
    description: str
    threshold: Optional[float] = None
    expected_value: Optional[Any] = None


class QualityChecks(BaseModel):
    """
    Container for all quality rules for one profile.

    Written to .pipedog/<profile>/checks.json by `pipedog init` and read back
    by `pipedog scan`. Storing rules separately from the schema lets users
    edit thresholds by hand or via `pipedog checks add/edit` without touching
    the statistical snapshot.

    Attributes:
        file:         Absolute path to the source file(s) rules were derived from.
        checks:       List of individual QualityCheck rules.
        generated_at: ISO-8601 UTC timestamp of when the rules were created.
    """

    file: str
    checks: list[QualityCheck]
    generated_at: str


class CheckResult(BaseModel):
    """
    The outcome of evaluating a single quality rule or drift check.

    Produced by scanner.py and consumed by output.py and reporter.py to render
    the terminal and HTML reports. Results are not persisted to disk directly
    (they are summarised into ScanHistoryEntry for history.json).

    Attributes:
        column:      Column name (or "__row_count__" for row count check).
        check_type:  Mirrors QualityCheck.check_type, or "schema_drift" /
                     "type_change" for structural drift findings.
        description: Short human-readable summary of the rule.
        passed:      True if the rule was satisfied.
        detail:      Full plain-English sentence explaining the outcome.
        severity:    "error"   — causes overall scan to fail (exit code 1).
                     "warning" — reported but does not fail the scan.
    """

    column: str
    check_type: str
    description: str
    passed: bool
    detail: str
    severity: str = "error"  # "error" → scan fails; "warning" → scan passes with notice


class ScanHistoryEntry(BaseModel):
    """
    A single row in the per-profile scan audit log (history.json).

    One entry is appended after every successful `pipedog scan` run,
    regardless of whether the scan passed or failed. This provides a
    complete audit trail for compliance and trend analysis.

    Attributes:
        timestamp:      ISO-8601 UTC timestamp of the scan.
        file_scanned:   Absolute path of the file that was scanned.
        profile:        Profile name used ("default" if none specified).
        overall_passed: True if the scan exited with code 0.
        total_checks:   Total number of checks evaluated (drift + quality).
        passed_count:   Number of checks that passed.
        warning_count:  Number of warning-severity failures.
        failed_count:   Number of error-severity failures.
        drift_count:    Number of structural drift findings (added/removed/changed).
        report_path:    Absolute path to the HTML report file, if one was generated.
    """

    timestamp: str
    file_scanned: str
    profile: str
    overall_passed: bool
    total_checks: int
    passed_count: int
    warning_count: int
    failed_count: int
    drift_count: int
    report_path: Optional[str] = None


class ScanHistory(BaseModel):
    """
    The complete scan audit log for one profile.

    Serialised as .pipedog/<profile>/history.json. Entries are appended
    chronologically; the list is never truncated automatically.

    Attributes:
        profile: Profile name this history belongs to.
        entries: Chronological list of scan results.
    """

    profile: str
    entries: list[ScanHistoryEntry]
