"""
output.py — Rich-formatted terminal output for all Pipedog commands.

All user-visible terminal output goes through this module. Keeps presentation
completely separate from business logic (profiling, drift detection, checks).
Functions here only receive model objects and render them — they never
compute statistics or make pass/fail decisions.

Colour conventions:
    green  — healthy / passing
    red    — error / failing (causes exit code 1)
    yellow — warning / changed (scan still passes)
    cyan   — informational labels and file paths
    dim    — secondary info (timestamps, sample values, hints)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.text import Text

from .schema import CheckResult, DataSchema, QualityChecks

# Shared Console instance — imported by main.py so all output uses the same
# stream with consistent settings across every command.
console = Console()


def print_profile(schema: DataSchema) -> None:
    """
    Render the full data profile table for a file.

    Displays:
      - A header panel with the file path.
      - A summary table: row count, column count, snapshot timestamp.
      - A per-column table: type, null stats (red if > 0), unique count,
        min/max, and up to 3 sample values.

    Called by both `pipedog init` (after saving the snapshot) and
    `pipedog profile` (read-only, no snapshot saved).

    Args:
        schema: The DataSchema produced by profiler.profile_dataframe().
    """
    console.print()
    if schema.source_files and len(schema.source_files) > 1:
        file_display = "\n".join(f"[bold cyan]{f}[/bold cyan]" for f in schema.source_files)
        title_label = f"[bold]Data Profile -- {len(schema.source_files)} files merged[/bold]"
    else:
        file_display = f"[bold cyan]{schema.file}[/bold cyan]"
        title_label = "[bold]Data Profile[/bold]"
    console.print(Panel(file_display, title=title_label, border_style="cyan"))

    summary = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
    summary.add_column("Key", style="dim")
    summary.add_column("Value", style="bold")
    summary.add_row("Rows", f"{schema.row_count:,}")
    summary.add_row("Columns", str(schema.column_count))
    summary.add_row(
        "Snapshot taken",
        schema.captured_at[:19].replace("T", " ") + " UTC",
    )
    if schema.source_files and len(schema.source_files) > 1:
        summary.add_row("Source files", str(len(schema.source_files)))
    console.print(summary)

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
    table.add_column("Sample Values", style="dim", min_width=12)

    for col in schema.columns:
        null_style = "red" if col.null_count > 0 else "green"
        min_str = f"{col.min_value:,.4g}" if col.min_value is not None else "-"
        max_str = f"{col.max_value:,.4g}" if col.max_value is not None else "-"
        sample_str = ", ".join(str(v) for v in col.sample_values[:3])
        unique_str = str(col.unique_count) if col.unique_count != -1 else "merged"
        table.add_row(
            col.name,
            col.dtype,
            Text(str(col.null_count), style=null_style),
            Text(f"{col.null_pct}%", style=null_style),
            unique_str,
            min_str,
            max_str,
            sample_str,
        )

    console.print(table)
    console.print()


def print_init_success(
    schema: DataSchema,
    num_checks: int,
    profile: Optional[str] = None,
) -> None:
    """
    Print the success banner after `pipedog init` completes.

    Shown after the profile table. Confirms what was saved, which profile
    was used, and how many quality rules were auto-generated.

    Args:
        schema:     The DataSchema persisted to schema.json.
        num_checks: Number of QualityCheck rules written to checks.json.
        profile:    Profile name, or None for the default profile.
    """
    profile_dir = f".pipedog/{profile}" if profile else ".pipedog"
    source_note = ""
    if schema.source_files and len(schema.source_files) > 1:
        source_note = f"\n[green]{len(schema.source_files)} files merged into baseline[/green]"

    console.print()
    console.print(Panel(
        f"[bold green]Schema snapshot saved to "
        f"[cyan]{profile_dir}/schema.json[/cyan][/bold green]{source_note}\n"
        f"[green]{schema.column_count} columns | {schema.row_count:,} rows | "
        f"{num_checks} quality checks generated[/green]",
        title="[bold green]Pipedog Initialized[/bold green]",
        border_style="green",
    ))
    console.print()


def print_scan_results(
    drift_results: list[CheckResult],
    check_results: list[CheckResult],
    current_schema: DataSchema,
    report_path: Optional[Path] = None,
) -> bool:
    """
    Render the full scan report and return whether the scan passed.

    Combines drift findings and quality check results into a single terminal
    report structured in four sections (only printed when non-empty):

        1. Schema Drift Detected  — added/removed columns, type changes
        2. Failed Checks          — error-severity rule failures
        3. Warnings               — warning-severity failures (scan still passes)
        4. Passed Checks          — all rules that were satisfied

    Pass/fail decision:
        PASS  — zero error-severity failures (warnings are allowed).
        FAIL  — one or more error-severity failures (exit code 1).

    Header panel colour:
        green  → all checks passed, no warnings
        yellow → all checks passed, warnings present
        red    → one or more checks failed

    Args:
        drift_results:  CheckResult list from scanner.detect_drift().
        check_results:  CheckResult list from scanner.run_quality_checks().
        current_schema: DataSchema for the current file (used in summary line).
        report_path:    Path to the HTML report if one was generated; shown
                        at the bottom of the output when provided.

    Returns:
        True if the scan passed (no error-severity failures), False otherwise.
    """
    all_results = drift_results + check_results
    failures = [r for r in all_results if not r.passed and r.severity == "error"]
    warnings = [r for r in all_results if not r.passed and r.severity == "warning"]
    passed = [r for r in all_results if r.passed]

    overall_pass = len(failures) == 0

    console.print()

    if overall_pass and not warnings:
        status_text = "[bold green]ALL CHECKS PASSED[/bold green]"
        border = "green"
    elif overall_pass:
        status_text = "[bold yellow]PASSED WITH WARNINGS[/bold yellow]"
        border = "yellow"
    else:
        status_text = "[bold red]CHECKS FAILED[/bold red]"
        border = "red"

    console.print(Panel(
        f"{status_text}\n"
        f"[dim]{current_schema.row_count:,} rows | "
        f"{current_schema.column_count} columns | "
        f"{len(passed)} passed | {len(warnings)} warnings | "
        f"{len(failures)} failed[/dim]",
        title="[bold]Pipedog Scan[/bold]",
        border_style=border,
    ))

    if drift_results:
        console.print("\n[bold yellow]Schema Drift Detected[/bold yellow]")
        for r in drift_results:
            icon = "[red]FAIL[/red]" if r.severity == "error" else "[yellow]WARN[/yellow]"
            console.print(f"  {icon}  {r.detail}")

    if failures:
        console.print("\n[bold red]Failed Checks[/bold red]")
        for r in failures:
            console.print(f"  [red]FAIL[/red]  {r.detail}")

    if warnings:
        console.print("\n[bold yellow]Warnings[/bold yellow]")
        for r in warnings:
            console.print(f"  [yellow]WARN[/yellow]  {r.detail}")

    if passed:
        console.print("\n[bold green]Passed Checks[/bold green]")
        for r in passed:
            console.print(f"  [green]PASS[/green]  {r.detail}")

    if report_path:
        console.print(
            f"\n[dim]HTML report saved: [cyan]{report_path}[/cyan][/dim]"
        )

    console.print()
    return overall_pass


def print_checks_table(
    checks: QualityChecks,
    profile: Optional[str] = None,
) -> None:
    """
    Render all quality check rules for a profile as a numbered Rich table.

    Used by `pipedog checks list`. Shows column, check type, description,
    and threshold/expected value so analysts can see exactly what will be
    evaluated on the next scan.

    Args:
        checks:  The QualityChecks loaded from checks.json.
        profile: Profile name to show in the table title.
    """
    profile_label = profile or "default"
    table = Table(
        title=f"Quality Checks - profile: {profile_label}",
        box=box.ROUNDED,
        header_style="bold magenta",
        show_lines=False,
    )
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Column", style="cyan", no_wrap=True)
    table.add_column("Check Type", style="yellow")
    table.add_column("Description")
    table.add_column("Threshold / Values", justify="right", style="dim")

    for i, c in enumerate(checks.checks, start=1):
        if c.threshold is not None:
            threshold_str = str(c.threshold)
        elif c.expected_value is not None:
            vals = c.expected_value
            threshold_str = (
                f"{len(vals)} values"
                if isinstance(vals, list) else str(vals)
            )
        else:
            threshold_str = "-"

        table.add_row(str(i), c.column, c.check_type, c.description, threshold_str)

    console.print()
    console.print(table)
    console.print(
        f"[dim]{len(checks.checks)} rules | generated {checks.generated_at[:10]}[/dim]\n"
    )
