"""
main.py — Pipedog CLI entry point (v0.4.0).

For a no-terminal alternative, launch the desktop GUI:
    pipedog-gui
    python -m pipedog.gui

CLI Commands:
    pipedog init <file> [<file2> ...] [--profile <name>]
        Profile one or more files and save a baseline snapshot.

    pipedog scan <file> [--profile <name>] [--no-report]
        Compare a file against the saved snapshot and run quality checks.
        Generates HTML and Excel reports automatically unless --no-report is given.

    pipedog profile <file>
        Show a data summary without saving anything.

    pipedog checks list   [--profile <name>]
        List all quality check rules for a profile.

    pipedog checks edit   [--profile <name>]
        Open checks.json in the system default editor (or print path).

    pipedog checks add    --column <col> --type <type> [--threshold <n>] [--profile <name>]
        Add a custom quality check rule.

    pipedog report [--profile <name>] [--last]
        List available HTML reports, or open the most recent one.

Exit codes:
    0 — success / all checks passed
    1 — failure / one or more error-severity checks failed
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer

from .history import append_scan_result
from .output import (
    console,
    print_checks_table,
    print_init_success,
    print_profile,
    print_scan_results,
)
from .profiler import (
    CHECKS_FILE,
    _pipedog_dir,
    generate_checks,
    load_file,
    load_snapshot,
    merge_schemas,
    profile_dataframe,
    save_snapshot,
)
from .reporter import (
    generate_excel_report,
    generate_html_report,
    open_last_report,
    save_excel_report,
    save_report,
)
from .scanner import detect_drift, run_quality_checks
from .schema import QualityCheck, QualityChecks

# ---------------------------------------------------------------------------
# Root app
# ---------------------------------------------------------------------------

app = typer.Typer(
    name="pipedog",
    help="Data quality and schema drift detection tool.",
    add_completion=False,
    rich_markup_mode="rich",
)

# ---------------------------------------------------------------------------
# checks sub-app
# ---------------------------------------------------------------------------

checks_app = typer.Typer(
    name="checks",
    help="View, edit, and add quality check rules.",
    add_completion=False,
    rich_markup_mode="rich",
)
app.add_typer(checks_app, name="checks")

# ---------------------------------------------------------------------------
# Shared option type aliases
# ---------------------------------------------------------------------------

ProfileOption = Annotated[
    Optional[str],
    typer.Option("--profile", "-p", help="Named profile (saved to .pipedog/<profile>/)"),
]


# ---------------------------------------------------------------------------
# pipedog init
# ---------------------------------------------------------------------------

@app.command()
def init(
    files: Annotated[
        list[str],
        typer.Argument(help="One or more CSV, Parquet, or JSON files to baseline"),
    ],
    profile: ProfileOption = None,
) -> None:
    """
    Profile one or more data files and save a baseline snapshot.

    Single file:
        pipedog init orders.csv
        pipedog init orders.csv --profile purchase

    Multiple files (merged baseline):
        pipedog init jan.csv feb.csv mar.csv --profile sales

    When multiple files are provided, Pipedog merges their statistics into
    one representative baseline. All files must have identical column names
    and types — a clear error is shown if they differ.

    Re-running init on the same profile overwrites the existing baseline.

    Exit codes:
        0 — snapshot saved successfully
        1 — file not found, unsupported type, or column mismatch across files
    """
    try:
        if len(files) == 1:
            console.print(f"[dim]Reading {files[0]}...[/dim]")
            df = load_file(files[0])
            schema = profile_dataframe(df, files[0])
        else:
            schemas = []
            for f in files:
                console.print(f"[dim]Reading {f}...[/dim]")
                df = load_file(f)
                schemas.append(profile_dataframe(df, f))
            console.print(f"[dim]Merging {len(files)} files into one baseline...[/dim]")
            schema = merge_schemas(schemas, files)

        checks = generate_checks(schema)
        save_snapshot(schema, checks, profile=profile)
        print_profile(schema)
        print_init_success(schema, len(checks.checks), profile=profile)

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# pipedog scan
# ---------------------------------------------------------------------------

@app.command()
def scan(
    file: Annotated[str, typer.Argument(help="Path to CSV, Parquet, or JSON file")],
    profile: ProfileOption = None,
    no_report: Annotated[
        bool,
        typer.Option("--no-report", help="Skip HTML report generation"),
    ] = False,
) -> None:
    """
    Compare a file against the saved snapshot and run quality checks.

    Requires a baseline created by `pipedog init`. Performs two checks:
      1. Schema drift — columns added/removed, type changes
      2. Quality checks — null rates, value ranges, row count, new categories

    An HTML report is saved automatically after every scan unless --no-report
    is specified. Use `pipedog report --last` to open the most recent report.

    Exit codes:
        0 — all checks passed (warnings are allowed)
        1 — one or more error-severity checks failed
    """
    try:
        console.print(f"[dim]Reading {file}...[/dim]")
        df = load_file(file)
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    try:
        baseline_schema, checks = load_snapshot(profile=profile)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    current_schema = profile_dataframe(df, file)
    drift_results = detect_drift(baseline_schema, current_schema)
    check_results = run_quality_checks(df, current_schema, checks)

    # Generate reports unless suppressed.
    report_path: Optional[Path] = None
    excel_report_path: Optional[Path] = None
    if not no_report:
        try:
            html = generate_html_report(
                drift_results, check_results,
                current_schema, baseline_schema,
                profile, file,
            )
            report_path = save_report(html, profile, file)
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not save HTML report: {e}")

        try:
            wb = generate_excel_report(
                drift_results, check_results,
                current_schema, baseline_schema,
                profile, file,
            )
            excel_report_path = save_excel_report(wb, profile, file)
        except Exception as e:
            console.print(f"[yellow]Warning:[/yellow] Could not save Excel report: {e}")

    # Append to scan history.
    try:
        append_scan_result(drift_results, check_results, file, profile, report_path)
    except Exception as e:
        console.print(f"[yellow]Warning:[/yellow] Could not update history: {e}")

    passed = print_scan_results(
        drift_results, check_results, current_schema,
        report_path, excel_report_path,
    )

    if not passed:
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# pipedog profile
# ---------------------------------------------------------------------------

@app.command()
def profile(
    file: Annotated[str, typer.Argument(help="Path to CSV, Parquet, or JSON file")],
) -> None:
    """
    Show a data summary without saving anything.

    Displays row count, column types, null counts, unique value counts,
    min/max ranges, and sample values. Nothing is written to disk.

    Use this to explore an unfamiliar file before committing to a baseline
    with `pipedog init`.

    Exit codes:
        0 — profile displayed successfully
        1 — file not found or unsupported file type
    """
    try:
        console.print(f"[dim]Reading {file}...[/dim]")
        df = load_file(file)
        schema = profile_dataframe(df, file)
        print_profile(schema)
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# pipedog report
# ---------------------------------------------------------------------------

@app.command()
def report(
    profile: ProfileOption = None,
    last: Annotated[
        bool,
        typer.Option("--last", help="Open the most recent HTML report in the browser"),
    ] = False,
) -> None:
    """
    List or open HTML scan reports.

    List all reports for a profile:
        pipedog report
        pipedog report --profile purchase

    Open the most recent report in the browser:
        pipedog report --last
        pipedog report --last --profile purchase
    """
    from rich.table import Table
    from rich import box as rbox

    if last:
        try:
            open_last_report(profile)
            console.print("[green]Opening report in browser...[/green]")
        except FileNotFoundError as e:
            console.print(f"[red]Error:[/red] {e}")
            raise typer.Exit(1)
        return

    # List available reports.
    reports_dir = _pipedog_dir(profile) / "reports"
    if not reports_dir.exists():
        console.print(
            "[yellow]No reports found.[/yellow] "
            "Run `pipedog scan` to generate one."
        )
        return

    report_files = sorted(
        reports_dir.glob("*.html"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not report_files:
        console.print(
            "[yellow]No reports found.[/yellow] "
            "Run `pipedog scan` to generate one."
        )
        return

    profile_label = profile or "default"
    tbl = Table(
        title=f"Scan Reports — profile: {profile_label}",
        box=rbox.ROUNDED,
        header_style="bold magenta",
    )
    tbl.add_column("#", style="dim", width=4, justify="right")
    tbl.add_column("Report File", style="cyan")
    tbl.add_column("Modified", style="dim")

    from datetime import datetime as _dt
    for i, r in enumerate(report_files, start=1):
        mtime = _dt.fromtimestamp(r.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        tbl.add_row(str(i), r.name, mtime)

    console.print()
    console.print(tbl)
    console.print(
        "[dim]Use [cyan]pipedog report --last[/cyan] to open the most recent report.[/dim]\n"
    )


# ---------------------------------------------------------------------------
# pipedog checks list
# ---------------------------------------------------------------------------

@checks_app.command("list")
def checks_list(
    profile: ProfileOption = None,
) -> None:
    """
    List all quality check rules for the given profile.

    Shows column, check type, description, and threshold so you can see
    exactly what will be evaluated on the next scan.
    """
    try:
        _, checks = load_snapshot(profile=profile)
        print_checks_table(checks, profile)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# pipedog checks edit
# ---------------------------------------------------------------------------

@checks_app.command("edit")
def checks_edit(
    profile: ProfileOption = None,
) -> None:
    """
    Open checks.json in the system default editor.

    If the $EDITOR environment variable is not set, prints the file path
    so you can open it manually in any text editor or IDE.

    After editing, changes take effect on the next `pipedog scan` — no
    reload step required.
    """
    import os
    import subprocess

    checks_path = _pipedog_dir(profile) / CHECKS_FILE
    if not checks_path.exists():
        profile_hint = f" --profile {profile}" if profile else ""
        console.print(
            f"[red]Error:[/red] No checks found. "
            f"Run `pipedog init <file>{profile_hint}` first."
        )
        raise typer.Exit(1)

    editor = os.environ.get("EDITOR") or os.environ.get("VISUAL")
    if editor:
        subprocess.run([editor, str(checks_path)])
    else:
        console.print(
            f"[bold]Checks file:[/bold] [cyan]{checks_path.resolve()}[/cyan]\n"
            "[dim]Set the $EDITOR environment variable to open it automatically.[/dim]"
        )


# ---------------------------------------------------------------------------
# pipedog checks add
# ---------------------------------------------------------------------------

@checks_app.command("add")
def checks_add(
    column: Annotated[str, typer.Option("--column", "-c", help="Column name the rule applies to")],
    type_: Annotated[str, typer.Option("--type", "-t", help="Check type (e.g. max_value, not_null)")],
    threshold: Annotated[
        Optional[float],
        typer.Option("--threshold", help="Numeric threshold for range/rate checks"),
    ] = None,
    profile: ProfileOption = None,
) -> None:
    """
    Add a custom quality check rule to checks.json.

    Examples:
        pipedog checks add --column price --type max_value --threshold 9999
        pipedog checks add --column status --type not_null
        pipedog checks add --column amount --type min_value --threshold 0 --profile purchase
    """
    try:
        _, checks = load_snapshot(profile=profile)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)

    description = f"Custom rule: '{column}' {type_}"
    if threshold is not None:
        description += f" threshold={threshold}"

    new_check = QualityCheck(
        column=column,
        check_type=type_,
        description=description,
        threshold=threshold,
    )
    checks.checks.append(new_check)

    checks_path = _pipedog_dir(profile) / CHECKS_FILE
    checks_path.write_text(checks.model_dump_json(indent=2))

    console.print(f"[green]Added rule:[/green] {description}")
    console.print(
        f"[dim]Total rules for profile '{profile or 'default'}': "
        f"{len(checks.checks)}[/dim]"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()
