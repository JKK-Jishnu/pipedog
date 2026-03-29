"""
main.py — Pipedog CLI entry point.

Defines the three top-level commands using Typer:

    pipedog init <file>     Profile a file and save a schema snapshot.
    pipedog scan <file>     Compare a file against the saved snapshot.
    pipedog profile <file>  Display a data profile without saving anything.

Each command follows the same pattern:
    1. Load the file into a DataFrame (raises on unsupported extension).
    2. Call the appropriate profiler / scanner functions.
    3. Render output via output.py.
    4. Exit with code 0 on success, code 1 on failure.

Error handling is deliberately user-friendly: raw exceptions are caught and
printed as plain English messages rather than tracebacks.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from .output import console, print_init_success, print_profile, print_scan_results
from .profiler import (
    generate_checks,
    load_file,
    load_snapshot,
    profile_dataframe,
    save_snapshot,
)
from .scanner import detect_drift, run_quality_checks

# The root Typer application. `add_completion=False` keeps the CLI surface
# minimal — shell completion can be added later when the tool matures.
app = typer.Typer(
    name="pipedog",
    help="Data quality and schema drift detection tool.",
    add_completion=False,
    rich_markup_mode="rich",
)


@app.command()
def init(
    file: Annotated[str, typer.Argument(help="Path to CSV, Parquet, or JSON file")],
) -> None:
    """
    Profile a data file and save a schema snapshot + quality checks to .pipedog/.

    This command must be run before `pipedog scan`. It does three things:

    1. Reads the file and computes per-column statistics (type, nullability,
       value ranges, unique counts, sample values).
    2. Auto-generates quality rules from those statistics (not-null, range,
       uniqueness checks).
    3. Saves the snapshot to .pipedog/schema.json and rules to
       .pipedog/checks.json so future `scan` runs have a baseline to compare
       against.

    Re-running `init` on the same directory overwrites the existing baseline.
    This is useful when you intentionally update the source schema and want
    to reset the baseline to the new structure.

    Exit codes:
        0 — profiling and snapshot saved successfully.
        1 — file not found or unsupported file type.
    """
    try:
        console.print(f"[dim]Reading {file}...[/dim]")
        df = load_file(file)
        schema = profile_dataframe(df, file)
        checks = generate_checks(schema)
        save_snapshot(schema, checks)
        print_profile(schema)
        print_init_success(schema, len(checks.checks))
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


@app.command()
def scan(
    file: Annotated[str, typer.Argument(help="Path to CSV, Parquet, or JSON file")],
) -> None:
    """
    Compare a file against the saved schema snapshot and run quality checks.

    Requires a baseline created by `pipedog init`. The scan does two things:

    1. Schema drift detection — compares column names and types against the
       baseline. Added columns produce warnings; removed columns and type
       changes produce errors.
    2. Quality check evaluation — runs every rule in .pipedog/checks.json
       against the current file (null rates, value ranges, uniqueness).

    Output is colour-coded:
        green  — all checks passed.
        yellow — passed with warnings (new columns, elevated null rates).
        red    — one or more checks failed.

    Exit codes:
        0 — all checks passed (warnings are allowed).
        1 — one or more error-severity checks failed, OR file/snapshot not found.
    """
    # Load the current file first so we can report file errors before snapshot errors.
    try:
        console.print(f"[dim]Reading {file}...[/dim]")
        df = load_file(file)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    # Load the baseline snapshot saved by `pipedog init`.
    try:
        baseline_schema, checks = load_snapshot()
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    # Profile the current file and compare against the baseline.
    current_schema = profile_dataframe(df, file)
    drift_results = detect_drift(baseline_schema, current_schema)
    check_results = run_quality_checks(df, current_schema, checks)

    passed = print_scan_results(drift_results, check_results, current_schema)

    if not passed:
        raise typer.Exit(1)


@app.command()
def profile(
    file: Annotated[str, typer.Argument(help="Path to CSV, Parquet, or JSON file")],
) -> None:
    """
    Show a summary of the data without saving anything.

    Displays row count, column types, null counts, unique value counts,
    min/max ranges, and sample values. Nothing is written to disk.

    Use this command when you want to quickly explore an unfamiliar file
    before committing to a baseline with `pipedog init`.

    Exit codes:
        0 — profile displayed successfully.
        1 — file not found or unsupported file type.
    """
    try:
        console.print(f"[dim]Reading {file}...[/dim]")
        df = load_file(file)
        schema = profile_dataframe(df, file)
        print_profile(schema)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
