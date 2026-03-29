"""
output.py — Rich-formatted terminal output for all three Pipedog commands.

All user-visible output goes through this module. It keeps presentation
completely separate from business logic (profiling, drift detection, checks).
Functions here only receive model objects and render them — they never
compute statistics or make pass/fail decisions.

Colour conventions used throughout:
    green  — healthy / passing
    red    — error / failing (causes exit code 1)
    yellow — warning / changed (scan still passes)
    cyan   — informational labels and file paths
    dim    — secondary information (timestamps, sample values)
"""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.text import Text

from .schema import CheckResult, ColumnSchema, DataSchema

# Shared Console instance used by all output functions and imported by main.py
# so that all output goes to the same stream with consistent settings.
console = Console()


def print_profile(schema: DataSchema) -> None:
    """
    Render the full data profile table for a file.

    Displays:
      - A header panel with the file path.
      - A summary table with row count, column count, and snapshot timestamp.
      - A per-column table with type, null stats, unique count, min/max,
        and up to 3 sample values.

    Null counts and percentages are coloured red when > 0, green when clean,
    so analysts can immediately spot columns that need attention.

    Called by both `pipedog init` (after saving the snapshot) and
    `pipedog profile` (read-only, no snapshot saved).

    Args:
        schema: The DataSchema produced by profiler.profile_dataframe().
    """
    console.print()
    console.print(
        Panel(
            f"[bold cyan]{schema.file}[/bold cyan]",
            title="[bold]Data Profile[/bold]",
            border_style="cyan",
        )
    )

    # Top-level summary: row/column counts and when the profile was captured.
    summary = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
    summary.add_column("Key", style="dim")
    summary.add_column("Value", style="bold")
    summary.add_row("Rows", f"{schema.row_count:,}")
    summary.add_row("Columns", str(schema.column_count))
    summary.add_row("Snapshot taken", schema.captured_at[:19].replace("T", " ") + " UTC")
    console.print(summary)

    # Per-column detail table.
    table = Table(
        title="Column Summary",
        box=box.ROUNDED,
        header_style="bold magenta",
        show_lines=False,
    )
    table.add_column("Column", style="cyan", no_wrap=True)
    table.add_column("Type", style="yellow")
    table.add_column("Nulls", justify="right")
    table.add_column("Null %", justify="right")
    table.add_column("Unique", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("Sample Values", style="dim")

    for col in schema.columns:
        # Null-related cells are red when there are nulls, green when clean.
        null_style = "red" if col.null_count > 0 else "green"
        min_str = f"{col.min_value:,.4g}" if col.min_value is not None else "-"
        max_str = f"{col.max_value:,.4g}" if col.max_value is not None else "-"
        # Show up to 3 sample values as a comma-separated string.
        sample_str = ", ".join(str(v) for v in col.sample_values[:3])
        table.add_row(
            col.name,
            col.dtype,
            Text(str(col.null_count), style=null_style),
            Text(f"{col.null_pct}%", style=null_style),
            str(col.unique_count),
            min_str,
            max_str,
            sample_str,
        )

    console.print(table)
    console.print()


def print_init_success(schema: DataSchema, num_checks: int) -> None:
    """
    Print the success banner after `pipedog init` completes.

    Shown after the profile table. Confirms what was saved and how many
    quality rules were auto-generated from the baseline.

    Args:
        schema:     The DataSchema that was persisted to .pipedog/schema.json.
        num_checks: The number of QualityCheck rules written to checks.json.
    """
    console.print()
    console.print(
        Panel(
            f"[bold green]Schema snapshot saved to [cyan].pipedog/schema.json[/cyan][/bold green]\n"
            f"[green]{schema.column_count} columns · {schema.row_count:,} rows · "
            f"{num_checks} quality checks generated[/green]",
            title="[bold green]Pipedog Initialized[/bold green]",
            border_style="green",
        )
    )
    console.print()


def print_scan_results(
    drift_results: list[CheckResult],
    check_results: list[CheckResult],
    current_schema: DataSchema,
) -> bool:
    """
    Render the full scan report and return whether the scan passed.

    Combines drift findings (structural changes) and quality check results
    into a single terminal report. The report is structured in four sections,
    printed only when non-empty:

        1. Schema Drift Detected  — added/removed columns, type changes.
        2. Failed Checks          — error-severity rule failures.
        3. Warnings               — warning-severity failures (scan still passes).
        4. Passed Checks          — all rules that were satisfied.

    The overall pass/fail decision:
        PASS  — zero error-severity failures (warnings are allowed).
        FAIL  — one or more error-severity failures (exit code 1).

    The header panel colour reflects the overall state:
        green  → all checks passed, no warnings.
        yellow → all checks passed, but there are warnings.
        red    → one or more checks failed.

    Args:
        drift_results:  CheckResult list from scanner.detect_drift().
        check_results:  CheckResult list from scanner.run_quality_checks().
        current_schema: The DataSchema for the current file (used for
                        the row/column count in the header summary).

    Returns:
        True if the scan passed (no error-severity failures), False otherwise.
    """
    all_results = drift_results + check_results

    # Partition results into three buckets for display.
    failures = [r for r in all_results if not r.passed and r.severity == "error"]
    warnings = [r for r in all_results if not r.passed and r.severity == "warning"]
    passed = [r for r in all_results if r.passed]

    # Warnings alone do not fail the scan.
    overall_pass = len(failures) == 0

    console.print()

    # Choose header colour based on overall outcome.
    if overall_pass and not warnings:
        status_text = "[bold green]ALL CHECKS PASSED[/bold green]"
        border = "green"
    elif overall_pass:
        status_text = "[bold yellow]PASSED WITH WARNINGS[/bold yellow]"
        border = "yellow"
    else:
        status_text = "[bold red]CHECKS FAILED[/bold red]"
        border = "red"

    console.print(
        Panel(
            f"{status_text}\n"
            f"[dim]{current_schema.row_count:,} rows · {current_schema.column_count} columns · "
            f"{len(passed)} passed · {len(warnings)} warnings · {len(failures)} failed[/dim]",
            title="[bold]Pipedog Scan[/bold]",
            border_style=border,
        )
    )

    # Section 1: schema drift (added/removed columns, type changes).
    if drift_results:
        console.print("\n[bold yellow]Schema Drift Detected[/bold yellow]")
        for r in drift_results:
            icon = "[red]FAIL[/red]" if r.severity == "error" else "[yellow]WARN[/yellow]"
            console.print(f"  {icon}  {r.detail}")

    # Section 2: failed quality checks (error severity only).
    if failures:
        console.print("\n[bold red]Failed Checks[/bold red]")
        for r in failures:
            console.print(f"  [red]FAIL[/red]  {r.detail}")

    # Section 3: warning-severity failures.
    if warnings:
        console.print("\n[bold yellow]Warnings[/bold yellow]")
        for r in warnings:
            console.print(f"  [yellow]WARN[/yellow]  {r.detail}")

    # Section 4: passing checks (shown last so failures are easier to spot).
    if passed:
        console.print("\n[bold green]Passed Checks[/bold green]")
        for r in passed:
            console.print(f"  [green]PASS[/green]  {r.detail}")

    console.print()
    return overall_pass
