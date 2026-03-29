"""
schema.py — Pydantic data models for Pipedog.

All data flowing between the profiler, scanner, and output layers is typed
using these models. Pydantic validates the data at construction time and
handles JSON serialisation/deserialisation when reading/writing .pipedog/.
"""

from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel


class ColumnSchema(BaseModel):
    """
    Statistics for a single column captured during `pipedog init`.

    These values become the baseline that future `pipedog scan` runs
    compare against. Every field is JSON-serialisable so the snapshot
    can be stored in .pipedog/schema.json.

    Attributes:
        name:          Column name as it appears in the file header.
        dtype:         Human-readable type: "integer", "float", "string",
                       "boolean", or "datetime".
        nullable:      True if at least one null/NaN was found at init time.
        null_count:    Absolute number of null rows.
        null_pct:      Null rows as a percentage of total rows (0–100).
        unique_count:  Number of distinct non-null values.
        sample_values: Up to 5 representative non-null values; used in the
                       profile table displayed to the user.
        min_value:     Numeric minimum (None for non-numeric columns).
        max_value:     Numeric maximum (None for non-numeric columns).
        mean_value:    Numeric mean rounded to 4 decimal places
                       (None for non-numeric columns).
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


class DataSchema(BaseModel):
    """
    Full schema snapshot for a data file, written to .pipedog/schema.json.

    Captures both file-level metadata (row/column counts, timestamp) and
    per-column statistics (via ColumnSchema). This is the baseline that
    `pipedog scan` compares new files against.

    Attributes:
        file:         Absolute path to the file at init time.
        row_count:    Total number of rows in the file.
        column_count: Total number of columns.
        columns:      Ordered list of per-column statistics.
        captured_at:  ISO-8601 UTC timestamp of when the snapshot was taken.
    """

    file: str
    row_count: int
    column_count: int
    columns: list[ColumnSchema]
    captured_at: str


class QualityCheck(BaseModel):
    """
    A single auto-generated quality rule, written to .pipedog/checks.json.

    Rules are generated from the baseline snapshot by `generate_checks()` in
    profiler.py and evaluated against new data by `run_quality_checks()` in
    scanner.py.

    Supported check_type values:
        "not_null"   — column must have zero nulls (baseline was null-free).
        "null_rate"  — null % must stay below threshold (baseline had nulls,
                       threshold = baseline_pct + 10 percentage points).
        "min_value"  — numeric minimum must be >= threshold.
        "max_value"  — numeric maximum must be <= threshold.
        "unique"     — every value must be distinct (column looked like a key).

    Attributes:
        column:         Name of the column the rule applies to.
        check_type:     One of the type strings listed above.
        description:    Plain-English description shown in the terminal.
        threshold:      Numeric threshold used for range / null-rate checks.
        expected_value: Reserved for future categorical / set-membership checks.
    """

    column: str
    check_type: str
    description: str
    threshold: Optional[float] = None
    expected_value: Optional[Any] = None


class QualityChecks(BaseModel):
    """
    Container for all auto-generated quality rules for a file.

    Written to .pipedog/checks.json by `pipedog init` and read back by
    `pipedog scan`. Storing them separately from the schema lets users edit
    rules by hand without touching the schema snapshot.

    Attributes:
        file:         Absolute path to the source file these rules were derived from.
        checks:       List of individual QualityCheck rules.
        generated_at: ISO-8601 UTC timestamp of when the rules were created.
    """

    file: str
    checks: list[QualityCheck]
    generated_at: str


class CheckResult(BaseModel):
    """
    The outcome of evaluating a single quality rule or drift check.

    Produced by scanner.py and consumed by output.py to render the
    colour-coded terminal report. Results are not persisted to disk.

    Attributes:
        column:      Name of the column that was checked.
        check_type:  Mirrors QualityCheck.check_type, or "schema_drift" /
                     "type_change" for structural drift findings.
        description: Short human-readable summary of the rule.
        passed:      True if the rule was satisfied.
        detail:      Full plain-English sentence explaining the outcome,
                     including the actual value that was observed.
        severity:    "error" — causes overall scan to fail (exit code 1).
                     "warning" — reported but does not fail the scan.
    """

    column: str
    check_type: str
    description: str
    passed: bool
    detail: str
    severity: str = "error"  # "error" → scan fails; "warning" → scan passes with notice
