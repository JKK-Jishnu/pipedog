"""
history.py — Scan history persistence.

Appends one ScanHistoryEntry to .pipedog/<profile>/history.json after every
`pipedog scan` run, regardless of whether the scan passed or failed.

This provides:
  - A complete audit trail for compliance teams.
  - Raw data for future trend analysis (null rate over time, etc.).
  - A record of which HTML report corresponds to which scan.

The history file is append-only (entries are never removed automatically).
It is plain JSON and human-readable.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .profiler import _pipedog_dir
from .schema import CheckResult, ScanHistory, ScanHistoryEntry

HISTORY_FILE = "history.json"


def load_history(profile: Optional[str] = None) -> ScanHistory:
    """
    Load the scan history for the given profile.

    Returns an empty ScanHistory if history.json does not exist yet
    (i.e. before the first scan). Never raises FileNotFoundError.

    Args:
        profile: Profile name, or None for the default profile.

    Returns:
        A ScanHistory model (possibly with an empty entries list).
    """
    path = _pipedog_dir(profile) / HISTORY_FILE
    if not path.exists():
        return ScanHistory(profile=profile or "default", entries=[])
    return ScanHistory.model_validate_json(path.read_text())


def append_scan_result(
    drift_results: list[CheckResult],
    check_results: list[CheckResult],
    file_scanned: str,
    profile: Optional[str],
    report_path: Optional[Path] = None,
) -> ScanHistoryEntry:
    """
    Build a ScanHistoryEntry from scan results and append it to history.json.

    Creates history.json if it does not exist. The full history is read,
    the new entry appended, and the file rewritten atomically (write to temp
    then rename is not used here — single-user CLI tool, no concurrency risk).

    Args:
        drift_results: CheckResult list from scanner.detect_drift().
        check_results: CheckResult list from scanner.run_quality_checks().
        file_scanned:  Path to the file that was scanned.
        profile:       Profile name, or None for the default profile.
        report_path:   Path to the HTML report, if one was generated.

    Returns:
        The ScanHistoryEntry that was appended.
    """
    all_results = drift_results + check_results
    failures = [r for r in all_results if not r.passed and r.severity == "error"]
    warnings_list = [r for r in all_results if not r.passed and r.severity == "warning"]
    passed_list = [r for r in all_results if r.passed]

    entry = ScanHistoryEntry(
        timestamp=datetime.now(timezone.utc).isoformat(),
        file_scanned=str(Path(file_scanned).resolve()),
        profile=profile or "default",
        overall_passed=len(failures) == 0,
        total_checks=len(all_results),
        passed_count=len(passed_list),
        warning_count=len(warnings_list),
        failed_count=len(failures),
        drift_count=len(drift_results),
        report_path=str(report_path) if report_path else None,
    )

    history = load_history(profile)
    history.entries.append(entry)

    history_path = _pipedog_dir(profile) / HISTORY_FILE
    history_path.write_text(history.model_dump_json(indent=2))

    return entry
