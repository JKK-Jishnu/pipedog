"""
reporter.py — HTML scan report generation.

Generates a self-contained, human-readable HTML report after every
`pipedog scan`. Reports have zero external dependencies at render time —
all CSS is inline, no JavaScript frameworks, no CDN requests.

Reports are saved to:
    .pipedog/<profile>/reports/<stem>-<profile>-<YYYYMMDD-HHMMSS>.html

Key functions:
    generate_html_report()  Build the HTML string from scan results.
    save_report()           Write the HTML file and return its path.
    open_last_report()      Open the most recent report in the system browser.
"""

from __future__ import annotations

import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import __version__
from .profiler import _pipedog_dir
from .schema import CheckResult, DataSchema

# ---------------------------------------------------------------------------
# CSS (inline, no external dependencies)
# ---------------------------------------------------------------------------

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    font-size: 14px;
    color: #1f2937;
    background: #f9fafb;
    padding: 24px;
}
h1 { font-size: 22px; font-weight: 700; margin-bottom: 4px; }
h2 { font-size: 15px; font-weight: 600; margin: 24px 0 10px; }
.meta { color: #6b7280; font-size: 12px; margin-bottom: 20px; }
.summary {
    border-radius: 8px;
    padding: 16px 20px;
    margin-bottom: 24px;
    border-left: 6px solid #ccc;
    background: #fff;
}
.summary.pass  { border-color: #16a34a; background: #f0fdf4; }
.summary.warn  { border-color: #d97706; background: #fffbeb; }
.summary.fail  { border-color: #dc2626; background: #fef2f2; }
.summary .status { font-size: 18px; font-weight: 700; margin-bottom: 6px; }
.summary.pass .status  { color: #16a34a; }
.summary.warn .status  { color: #d97706; }
.summary.fail .status  { color: #dc2626; }
.summary .counts { color: #6b7280; font-size: 13px; }
table {
    width: 100%;
    border-collapse: collapse;
    background: #fff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    margin-bottom: 8px;
}
th {
    background: #f3f4f6;
    padding: 10px 14px;
    text-align: left;
    font-size: 12px;
    font-weight: 600;
    color: #6b7280;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
td { padding: 10px 14px; border-top: 1px solid #f3f4f6; font-size: 13px; }
tr:hover td { background: #f9fafb; }
.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.03em;
}
.badge-pass { background: #dcfce7; color: #16a34a; }
.badge-fail { background: #fee2e2; color: #dc2626; }
.badge-warn { background: #fef3c7; color: #d97706; }
.badge-drift { background: #e0e7ff; color: #4338ca; }
.section-empty { color: #9ca3af; font-style: italic; padding: 8px 0; }
.report-path { font-size: 11px; color: #9ca3af; margin-top: 32px; }
"""

# ---------------------------------------------------------------------------
# HTML builders
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """HTML-escape a string to prevent XSS in report values."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _badge(label: str, kind: str) -> str:
    return f'<span class="badge badge-{kind}">{_esc(label)}</span>'


def _results_table(results: list[CheckResult], include_status: bool = True) -> str:
    if not results:
        return '<p class="section-empty">None.</p>'
    rows = ""
    for r in results:
        if r.passed:
            status = _badge("PASS", "pass")
        elif r.severity == "warning":
            status = _badge("WARN", "warn")
        else:
            status = _badge("FAIL", "fail")

        col_display = r.column if r.column != "__row_count__" else "File (row count)"
        rows += (
            f"<tr>"
            f"<td>{_esc(col_display)}</td>"
            f"<td>{_esc(r.check_type)}</td>"
            f"<td>{status}</td>"
            f"<td>{_esc(r.detail)}</td>"
            f"</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Column</th><th>Check Type</th><th>Status</th><th>Detail</th>"
        "</tr></thead><tbody>"
        + rows
        + "</tbody></table>"
    )


def _drift_table(drift_results: list[CheckResult]) -> str:
    if not drift_results:
        return '<p class="section-empty">No structural drift detected.</p>'
    rows = ""
    for r in drift_results:
        sev = _badge("WARN", "warn") if r.severity == "warning" else _badge("FAIL", "fail")
        rows += (
            f"<tr>"
            f"<td>{_esc(r.column)}</td>"
            f"<td>{_badge(r.check_type.replace('_', ' ').title(), 'drift')}</td>"
            f"<td>{sev}</td>"
            f"<td>{_esc(r.detail)}</td>"
            f"</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Column</th><th>Type</th><th>Severity</th><th>Detail</th>"
        "</tr></thead><tbody>"
        + rows
        + "</tbody></table>"
    )


def _column_profile_table(schema: DataSchema) -> str:
    rows = ""
    for col in schema.columns:
        null_color = "#dc2626" if col.null_count > 0 else "#16a34a"
        null_cell = (
            f'<td style="color:{null_color}">'
            f"{col.null_count} ({col.null_pct}%)</td>"
        )
        min_str = f"{col.min_value:,.4g}" if col.min_value is not None else "-"
        max_str = f"{col.max_value:,.4g}" if col.max_value is not None else "-"
        std_str = f"{col.std_dev:,.4g}" if col.std_dev is not None else "-"
        sample_str = _esc(", ".join(str(v) for v in col.sample_values[:3]))
        unique_str = str(col.unique_count) if col.unique_count != -1 else "merged"
        rows += (
            f"<tr>"
            f"<td><strong>{_esc(col.name)}</strong></td>"
            f"<td>{_esc(col.dtype)}</td>"
            f"{null_cell}"
            f"<td>{unique_str}</td>"
            f"<td>{min_str}</td>"
            f"<td>{max_str}</td>"
            f"<td>{std_str}</td>"
            f"<td>{sample_str}</td>"
            f"</tr>"
        )
    return (
        "<table><thead><tr>"
        "<th>Column</th><th>Type</th><th>Nulls</th>"
        "<th>Unique</th><th>Min</th><th>Max</th><th>Std Dev</th>"
        "<th>Sample Values</th>"
        "</tr></thead><tbody>"
        + rows
        + "</tbody></table>"
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_html_report(
    drift_results: list[CheckResult],
    check_results: list[CheckResult],
    current_schema: DataSchema,
    baseline_schema: DataSchema,
    profile: Optional[str],
    scanned_file: str,
) -> str:
    """
    Build a self-contained HTML scan report as a string.

    The report contains five sections:
        1. Summary header (pass/warn/fail banner with counts)
        2. Schema Drift (added/removed columns, type changes)
        3. Quality Check Results (all checks in one table)
        4. Column Profile (stats for every column in the scanned file)

    All styling is inline CSS — no external resources required.

    Args:
        drift_results:   CheckResult list from scanner.detect_drift().
        check_results:   CheckResult list from scanner.run_quality_checks().
        current_schema:  DataSchema profiled from the current file.
        baseline_schema: DataSchema loaded from the saved snapshot.
        profile:         Profile name, or None for the default profile.
        scanned_file:    Path to the file that was scanned.

    Returns:
        A complete HTML document as a string.
    """
    all_results = drift_results + check_results
    failures = [r for r in all_results if not r.passed and r.severity == "error"]
    warnings_list = [r for r in all_results if not r.passed and r.severity == "warning"]
    passed_list = [r for r in all_results if r.passed]

    overall_pass = len(failures) == 0

    if overall_pass and not warnings_list:
        status_class = "pass"
        status_label = "ALL CHECKS PASSED"
    elif overall_pass:
        status_class = "warn"
        status_label = "PASSED WITH WARNINGS"
    else:
        status_class = "fail"
        status_label = "CHECKS FAILED"

    profile_label = profile or "default"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    counts_str = (
        f"{current_schema.row_count:,} rows &nbsp;·&nbsp; "
        f"{current_schema.column_count} columns &nbsp;·&nbsp; "
        f"{len(passed_list)} passed &nbsp;·&nbsp; "
        f"{len(warnings_list)} warnings &nbsp;·&nbsp; "
        f"{len(failures)} failed"
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pipedog Scan — {_esc(Path(scanned_file).name)}</title>
<style>{_CSS}</style>
</head>
<body>

<h1>Pipedog Scan Report</h1>
<p class="meta">
  File: <strong>{_esc(scanned_file)}</strong> &nbsp;·&nbsp;
  Profile: <strong>{_esc(profile_label)}</strong> &nbsp;·&nbsp;
  {_esc(timestamp)}
</p>

<div class="summary {status_class}">
  <div class="status">{status_label}</div>
  <div class="counts">{counts_str}</div>
</div>

<h2>Schema Drift</h2>
{_drift_table(drift_results)}

<h2>Quality Checks</h2>
{_results_table(all_results)}

<h2>Column Profile (Current File)</h2>
{_column_profile_table(current_schema)}

<p class="report-path">
  Baseline snapshot: {_esc(baseline_schema.captured_at[:19].replace("T", " "))} UTC
  &nbsp;&middot;&nbsp; Generated by Pipedog v{__version__}
</p>

</body>
</html>"""

    return html


def save_report(
    html: str,
    profile: Optional[str],
    scanned_file: str,
) -> Path:
    """
    Write an HTML report string to the reports directory and return its path.

    File naming pattern:
        .pipedog/<profile>/reports/<stem>-<profile>-<YYYYMMDD-HHMMSS>.html

    Creates the reports/ directory if it does not exist.

    Args:
        html:         The HTML string from generate_html_report().
        profile:      Profile name, or None for the default profile.
        scanned_file: Path to the scanned file (used to derive the stem).

    Returns:
        The absolute Path of the written HTML file.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    stem = Path(scanned_file).stem
    profile_label = profile or "default"
    filename = f"{stem}-{profile_label}-{timestamp}.html"

    reports_dir = _pipedog_dir(profile) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    report_path = reports_dir / filename
    report_path.write_text(html, encoding="utf-8")
    return report_path.resolve()


def open_last_report(profile: Optional[str]) -> None:
    """
    Open the most recently modified HTML report in the system default browser.

    Args:
        profile: Profile name, or None for the default profile.

    Raises:
        FileNotFoundError: If no reports exist for the given profile.
    """
    reports_dir = _pipedog_dir(profile) / "reports"
    if not reports_dir.exists():
        raise FileNotFoundError(
            f"No reports directory found at {reports_dir}. "
            "Run `pipedog scan` first."
        )
    reports = sorted(
        reports_dir.glob("*.html"),
        key=lambda p: p.stat().st_mtime,
    )
    if not reports:
        raise FileNotFoundError(
            f"No HTML reports found in {reports_dir}. "
            "Run `pipedog scan` first."
        )
    webbrowser.open(reports[-1].resolve().as_uri())
