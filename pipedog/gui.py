"""
gui.py — Tkinter desktop GUI for Pipedog.

Launch with:
    python -m pipedog.gui
    pipedog-gui            (if installed via pip)
"""

from __future__ import annotations

import json
import os
import threading
import webbrowser
from datetime import datetime
from pathlib import Path
from tkinter import END, BooleanVar, StringVar, Tk, filedialog, messagebox
import tkinter as tk
import tkinter.ttk as ttk

CONFIG_FILE = Path.home() / ".pipedog_gui.json"

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------

CLR_PASS   = "#16a34a"
CLR_FAIL   = "#dc2626"
CLR_WARN   = "#d97706"
CLR_BG     = "#f9fafb"
CLR_PANEL  = "#ffffff"
CLR_HEADER = "#1f2937"
CLR_DIM    = "#9ca3af"
CLR_HINT   = "#b0b7c3"

PASS_BG = "#dcfce7"
FAIL_BG = "#fef2f2"
WARN_BG = "#fef3c7"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _short(path: str, max_len: int = 55) -> str:
    if len(path) <= max_len:
        return path
    return "…" + path[-(max_len - 1):]


def _pick_file(title: str = "Select a data file", multiple: bool = False):
    filetypes = [
        ("Data files", "*.csv *.xlsx *.xlsb *.parquet *.pq *.json"),
        ("All files", "*.*"),
    ]
    if multiple:
        return list(filedialog.askopenfilenames(title=title, filetypes=filetypes))
    return filedialog.askopenfilename(title=title, filetypes=filetypes) or ""


def _pick_dir(title: str = "Select working directory") -> str:
    return filedialog.askdirectory(title=title) or ""


def _open_file(path: Path) -> None:
    """Open a file with the system default application."""
    import os, subprocess, sys
    try:
        if sys.platform == "win32":
            os.startfile(str(path))
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception as exc:
        messagebox.showerror("Cannot open file", str(exc))


def _known_profiles(work_dir: str) -> list[str]:
    """Return profile names found under .pipedog/ in work_dir, always including 'default'."""
    profiles: set[str] = {"default"}
    try:
        pipedog_dir = Path(work_dir) / ".pipedog"
        if pipedog_dir.is_dir():
            for p in pipedog_dir.iterdir():
                if p.is_dir() and not p.name.startswith("."):
                    profiles.add(p.name)
    except Exception:
        pass
    return sorted(profiles)


def _rule_description(check_type: str, column: str, threshold: str) -> str:
    """Generate a human-readable rule description from check type + threshold."""
    t = threshold.strip()
    if check_type == "not_null":       return f"'{column}' must have no null values"
    if check_type == "null_rate":      return f"'{column}' null rate must stay below {t}%"
    if check_type == "min_value":      return f"'{column}' minimum value must be >= {t}"
    if check_type == "max_value":      return f"'{column}' maximum value must be <= {t}"
    if check_type == "row_count":      return f"File must have at least {t} rows"
    if check_type == "std_dev_change": return f"'{column}' std deviation must not change > {t}% from baseline"
    if check_type == "unique":         return f"'{column}' must contain only unique values"
    if check_type == "allowed_values": return f"'{column}' must only contain known values"
    return ""


def _pick_sheet(parent: tk.Tk, file_path: str) -> str | None:
    """Show a modal sheet-picker dialog for Excel files. Returns chosen sheet name or None."""
    try:
        from .profiler import get_sheet_names
        sheets = get_sheet_names(file_path)
    except Exception:
        return None
    if len(sheets) <= 1:
        return sheets[0] if sheets else None

    chosen: dict = {"sheet": sheets[0]}

    dlg = tk.Toplevel(parent)
    dlg.title("Choose Sheet")
    dlg.resizable(False, False)
    dlg.transient(parent)
    dlg.grab_set()

    frm = ttk.Frame(dlg, padding=14)
    frm.pack(fill="both", expand=True)

    ttk.Label(frm, text="This file has multiple sheets. Which one should be read?").pack(
        anchor="w", pady=(0, 8))

    var = StringVar(value=sheets[0])
    combo = ttk.Combobox(frm, textvariable=var, values=sheets, state="readonly", width=32)
    combo.pack(anchor="w", pady=(0, 12))

    def _ok() -> None:
        chosen["sheet"] = var.get()
        dlg.destroy()

    ttk.Button(frm, text="Continue", command=_ok, width=12).pack(anchor="e")
    dlg.wait_window()
    return chosen["sheet"]


# ---------------------------------------------------------------------------
# Reusable widgets
# ---------------------------------------------------------------------------

class StatusBanner(ttk.Frame):
    """Coloured pass/warn/fail banner that collapses when hidden."""

    def __init__(self, parent, **kw):
        super().__init__(parent, **kw)
        self._lbl = tk.Label(self, text="", font=("Segoe UI", 9, "bold"),
                             relief="flat", pady=5, padx=12, anchor="w")
        self._lbl.pack(fill="x")
        self.hide()

    def show(self, status: str, text: str) -> None:
        colours = {
            "pass": (PASS_BG, CLR_PASS),
            "warn": (WARN_BG, CLR_WARN),
            "fail": (FAIL_BG, CLR_FAIL),
        }
        bg, fg = colours.get(status, (CLR_PANEL, CLR_HEADER))
        self._lbl.config(text=text, background=bg, foreground=fg)
        self.pack_propagate(True)

    def hide(self) -> None:
        self.pack_propagate(False)
        self.configure(height=1)
        self._lbl.config(text="", background=CLR_BG)


class ResultsTree(ttk.Frame):
    """Treeview for check/drift results."""

    COLUMNS  = ("column", "check_type", "result", "detail")
    HEADINGS = ("Column", "Check", "Result", "Details")
    WIDTHS   = (130, 130, 60, 400)

    def __init__(self, parent, **kw):
        super().__init__(parent, **kw)
        tree = ttk.Treeview(self, columns=self.COLUMNS, show="headings", height=9)
        vsb = ttk.Scrollbar(self, orient="vertical",   command=tree.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        for col, head, w in zip(self.COLUMNS, self.HEADINGS, self.WIDTHS):
            tree.heading(col, text=head)
            tree.column(col, width=w, minwidth=50, stretch=(col == "detail"))
        tree.tag_configure("pass",  background=PASS_BG, foreground=CLR_PASS)
        tree.tag_configure("warn",  background=WARN_BG, foreground=CLR_WARN)
        tree.tag_configure("fail",  background=FAIL_BG, foreground=CLR_FAIL)
        tree.tag_configure("drift", background="#e0e7ff", foreground="#4338ca")
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.tree = tree

    def clear(self) -> None:
        self.tree.delete(*self.tree.get_children())

    def load(self, drift_results, check_results) -> None:
        self.clear()
        for r in drift_results + check_results:
            col_display = r.column if r.column != "__row_count__" else "File (row count)"
            if r.check_type in ("schema_drift", "type_change"):
                tag, status = "drift", ("Warn" if r.severity == "warning" else "Fail")
            elif r.passed:
                tag, status = "pass", "Pass"
            elif r.severity == "warning":
                tag, status = "warn", "Warn"
            else:
                tag, status = "fail", "Fail"
            self.tree.insert("", END,
                             values=(col_display, r.check_type, status, r.detail),
                             tags=(tag,))


class ProfileTree(ttk.Frame):
    """Treeview for column statistics."""

    COLUMNS  = ("name", "dtype", "nulls", "null_pct", "unique", "min", "max", "samples")
    HEADINGS = ("Column", "Type", "Nulls", "Null %", "Unique", "Min", "Max", "Sample Values")
    WIDTHS   = (130, 70, 55, 60, 60, 80, 80, 220)

    def __init__(self, parent, **kw):
        super().__init__(parent, **kw)
        tree = ttk.Treeview(self, columns=self.COLUMNS, show="headings", height=9)
        vsb = ttk.Scrollbar(self, orient="vertical",   command=tree.yview)
        hsb = ttk.Scrollbar(self, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        for col, head, w in zip(self.COLUMNS, self.HEADINGS, self.WIDTHS):
            tree.heading(col, text=head)
            tree.column(col, width=w, minwidth=40, stretch=(col == "samples"))
        tree.tag_configure("has_nulls", background=FAIL_BG, foreground=CLR_FAIL)
        tree.tag_configure("no_nulls",  background=CLR_PANEL)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.tree = tree

    def clear(self) -> None:
        self.tree.delete(*self.tree.get_children())

    def load(self, schema) -> None:
        self.clear()
        for col in schema.columns:
            min_s = f"{col.min_value:,.4g}" if col.min_value is not None else "—"
            max_s = f"{col.max_value:,.4g}" if col.max_value is not None else "—"
            samp  = ", ".join(str(v) for v in col.sample_values[:3])
            uniq  = str(col.unique_count) if col.unique_count != -1 else "merged"
            tag   = "has_nulls" if col.null_count > 0 else "no_nulls"
            self.tree.insert("", END, values=(
                col.name, col.dtype,
                col.null_count, f"{col.null_pct}%",
                uniq, min_s, max_s, samp,
            ), tags=(tag,))


# ---------------------------------------------------------------------------
# Tab 1 — Workspace
# ---------------------------------------------------------------------------

class WorkspaceTab(ttk.Frame):
    """
    Pick a file and run one of three actions:
      Create Baseline — save a snapshot to compare future files against
      Run Scan        — compare the file against a saved baseline
      Explore Data    — view column statistics without saving anything
    """

    def __init__(self, parent, app: "PipedogApp"):
        super().__init__(parent, padding=14)
        self.app = app
        self._file_path = ""
        self._sheet_name: str | None = None
        self._last_excel_path: Path | None = None
        self._build()

    def _build(self) -> None:
        # ── File picker ───────────────────────────────────────────────────
        top = ttk.Frame(self)
        top.pack(fill="x", pady=(0, 6))

        ttk.Label(top, text="File:", foreground=CLR_DIM).grid(
            row=0, column=0, sticky="w", padx=(0, 6))
        self._file_var = StringVar(value="No file chosen")
        self._file_lbl = ttk.Label(top, textvariable=self._file_var,
                                   foreground=CLR_HINT, width=46, anchor="w")
        self._file_lbl.grid(row=0, column=1, sticky="w", padx=(0, 10))
        ttk.Button(top, text="Browse…", command=self._browse).grid(
            row=0, column=2, padx=(0, 6))
        ttk.Button(top, text="Clear", command=self._clear).grid(
            row=0, column=3, padx=(0, 16))

        ttk.Label(top, text="Data starts at row:", foreground=CLR_DIM).grid(
            row=0, column=4, sticky="w", padx=(0, 4))
        self._skip_rows_var = StringVar(value="1")
        ttk.Spinbox(top, from_=1, to=999, width=5,
                    textvariable=self._skip_rows_var).grid(
            row=0, column=5, sticky="w")

        # ── Profile ───────────────────────────────────────────────────────
        mid = ttk.Frame(self)
        mid.pack(fill="x", pady=(0, 10))

        ttk.Label(mid, text="Profile:", foreground=CLR_DIM).grid(
            row=0, column=0, sticky="w", padx=(0, 6))
        self._profile_var = StringVar()
        self._profile_combo = ttk.Combobox(mid, textvariable=self._profile_var,
                                           width=22, state="normal")
        self._profile_combo["values"] = self.app.known_profiles()
        self._profile_combo.grid(row=0, column=1, sticky="w", padx=(0, 8))
        self.app._profile_combos.append(self._profile_combo)
        ttk.Label(mid, text="optional group name, e.g. 'sales'  —  blank uses default",
                  foreground=CLR_HINT,
                  font=("Segoe UI", 8)).grid(row=0, column=2, sticky="w")

        # ── Actions ───────────────────────────────────────────────────────
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill="x", pady=(0, 10))

        self._init_btn = ttk.Button(
            btn_frame, text="Create Baseline",
            command=self._run_init, width=18)
        self._init_btn.pack(side="left", padx=(0, 6))

        self._scan_btn = ttk.Button(
            btn_frame, text="Run Scan",
            command=self._run_scan, width=14)
        self._scan_btn.pack(side="left", padx=(0, 6))

        self._profile_btn = ttk.Button(
            btn_frame, text="Explore Data",
            command=self._run_profile, width=14)
        self._profile_btn.pack(side="left", padx=(0, 20))

        self._no_report = BooleanVar(value=False)
        ttk.Checkbutton(btn_frame, text="Don't save reports",
                        variable=self._no_report).pack(side="left", padx=(0, 20))

        self._open_report_btn = ttk.Button(
            btn_frame, text="Open Report",
            command=self._open_last_report, state="disabled")
        self._open_report_btn.pack(side="left")

        ttk.Separator(self, orient="horizontal").pack(fill="x", pady=(0, 8))

        # ── Status banner ─────────────────────────────────────────────────
        self._banner = StatusBanner(self)
        self._banner.pack(fill="x", pady=(0, 8))

        # ── Results ───────────────────────────────────────────────────────
        self._results_nb = ttk.Notebook(self)
        self._results_nb.pack(fill="both", expand=True)

        res_frame = ttk.Frame(self._results_nb, padding=2)
        self._results_nb.add(res_frame, text="Scan Results")
        self._results_tree = ResultsTree(res_frame)
        self._results_tree.pack(fill="both", expand=True)

        pro_frame = ttk.Frame(self._results_nb, padding=2)
        self._results_nb.add(pro_frame, text="Data Profile")
        self._profile_tree = ProfileTree(pro_frame)
        self._profile_tree.pack(fill="both", expand=True)
        ttk.Label(pro_frame,
                  text="Rows with a red background have one or more missing (null) values.",
                  foreground=CLR_DIM, font=("Segoe UI", 8)).pack(anchor="w", pady=(4, 0))

    # ── File helpers ───────────────────────────────────────────────────────

    def _browse(self) -> None:
        path = _pick_file("Select a data file")
        if not path:
            return
        self._file_path = path
        self._sheet_name = None
        ext = Path(path).suffix.lower()
        if ext in (".xlsx", ".xlsb"):
            sheet = _pick_sheet(self.app, path)
            self._sheet_name = sheet
            suffix = f"  [{sheet}]" if sheet else ""
        else:
            suffix = ""
        self._file_var.set(_short(path) + suffix)
        self._file_lbl.config(foreground=CLR_HEADER)

    def _clear(self) -> None:
        self._file_path = ""
        self._sheet_name = None
        self._file_var.set("No file chosen")
        self._file_lbl.config(foreground=CLR_HINT)

    def _get_profile(self) -> str | None:
        return self._profile_var.get().strip() or None

    def _get_skiprows(self) -> int:
        try:
            return max(0, int(self._skip_rows_var.get()) - 1)
        except ValueError:
            return 0

    def _set_buttons(self, state: str) -> None:
        for btn in (self._init_btn, self._scan_btn, self._profile_btn):
            btn.config(state=state)

    def _open_last_report(self) -> None:
        if self._last_excel_path and self._last_excel_path.exists():
            _open_file(self._last_excel_path)
        else:
            messagebox.showinfo("No report", "No report file found for the last scan.")

    # ── Create Baseline ────────────────────────────────────────────────────

    def _run_init(self) -> None:
        if not self._file_path:
            messagebox.showwarning("No file chosen", "Please choose a file first.")
            return
        self._set_buttons("disabled")
        self._banner.hide()
        self._results_tree.clear()
        self._profile_tree.clear()
        threading.Thread(
            target=self._worker_init,
            args=(self._file_path, self._get_profile(),
                  self._sheet_name, self._get_skiprows()),
            daemon=True,
        ).start()

    def _worker_init(self, file_path: str, profile,
                     sheet_name: str | None, skiprows: int) -> None:
        try:
            with self.app.work_dir():
                from .profiler import (
                    generate_checks, load_file, profile_dataframe, save_snapshot,
                )
                df     = load_file(file_path, sheet_name=sheet_name, skiprows=skiprows)
                schema = profile_dataframe(df, file_path)
                checks = generate_checks(schema)
                save_snapshot(schema, checks, profile=profile)
            self.app.after(0, lambda: self._on_init_done(schema, len(checks.checks), profile))
        except Exception as exc:
            msg = str(exc)
            self.app.after(0, lambda: self._on_error(msg))

    def _on_init_done(self, schema, num_checks: int, profile) -> None:
        self._set_buttons("normal")
        self._banner.show(
            "pass",
            f"Baseline created for \u2018{profile or 'default'}\u2019"
            f"   -  {schema.column_count} columns, {schema.row_count:,} rows,"
            f" {num_checks} checks saved",
        )
        self._profile_tree.load(schema)
        self._results_nb.select(1)
        self.app.remember_profile(profile)

    # ── Run Scan ───────────────────────────────────────────────────────────

    def _run_scan(self) -> None:
        if not self._file_path:
            messagebox.showwarning("No file chosen", "Please choose a file first.")
            return
        self._set_buttons("disabled")
        self._open_report_btn.config(state="disabled")
        self._last_excel_path = None
        self._banner.hide()
        self._results_tree.clear()
        self._profile_tree.clear()
        profile = self._get_profile()
        threading.Thread(
            target=self._worker_scan,
            args=(self._file_path, profile, self._no_report.get(),
                  self._sheet_name, self._get_skiprows()),
            daemon=True,
        ).start()

    def _worker_scan(self, file_path: str, profile, no_report: bool,
                     sheet_name: str | None, skiprows: int) -> None:
        try:
            with self.app.work_dir():
                from .profiler import load_file, load_snapshot, profile_dataframe
                from .scanner import detect_drift, run_quality_checks
                from .history import append_scan_result

                df               = load_file(file_path, sheet_name=sheet_name, skiprows=skiprows)
                baseline, checks = load_snapshot(profile=profile)
                current          = profile_dataframe(df, file_path)
                drift            = detect_drift(baseline, current)
                results          = run_quality_checks(df, current, checks)

                excel_path = None
                if not no_report:
                    try:
                        from .reporter import generate_excel_report, save_excel_report
                        wb = generate_excel_report(
                            drift, results, current, baseline, profile, file_path)
                        excel_path = save_excel_report(wb, profile, file_path)
                    except Exception:
                        pass
                report_path = excel_path  # used by history logger

                try:
                    append_scan_result(drift, results, file_path, profile, report_path)
                except Exception:
                    pass

            self.app.after(0, lambda: self._on_scan_done(
                drift, results, current, excel_path, profile))
        except Exception as exc:
            msg = str(exc)
            self.app.after(0, lambda: self._on_error(msg))

    def _on_scan_done(self, drift, results, current, excel_path, profile) -> None:
        self._set_buttons("normal")
        all_res  = drift + results
        failures = [r for r in all_res if not r.passed and r.severity == "error"]
        warnings = [r for r in all_res if not r.passed and r.severity == "warning"]
        passed   = [r for r in all_res if r.passed]

        counts = (
            f"{len(passed)} passed"
            + (f",  {len(warnings)} warnings" if warnings else "")
            + (f",  {len(failures)} failed" if failures else "")
        )

        if not failures and not warnings:
            status = "pass"
            msg = f"All checks passed   -  {current.row_count:,} rows,  {counts}"
        elif not failures:
            status = "warn"
            msg = f"Passed with warnings   -  {current.row_count:,} rows,  {counts}"
        else:
            status = "fail"
            msg = f"Checks failed   -  {current.row_count:,} rows,  {counts}"

        if excel_path:
            msg += "  \u00b7  report saved"
            self._last_excel_path = excel_path
            self._open_report_btn.config(state="normal")

        self._banner.show(status, msg)
        self._results_tree.load(drift, results)
        self._profile_tree.load(current)
        self._results_nb.select(0)
        self.app.remember_profile(profile)

    # ── Explore Data ───────────────────────────────────────────────────────

    def _run_profile(self) -> None:
        if not self._file_path:
            messagebox.showwarning("No file chosen", "Please choose a file first.")
            return
        self._set_buttons("disabled")
        self._banner.hide()
        self._profile_tree.clear()
        threading.Thread(
            target=self._worker_profile,
            args=(self._file_path, self._sheet_name, self._get_skiprows()),
            daemon=True,
        ).start()

    def _worker_profile(self, file_path: str,
                        sheet_name: str | None, skiprows: int) -> None:
        try:
            with self.app.work_dir():
                from .profiler import load_file, profile_dataframe
                df     = load_file(file_path, sheet_name=sheet_name, skiprows=skiprows)
                schema = profile_dataframe(df, file_path)
            self.app.after(0, lambda: self._on_profile_done(schema))
        except Exception as exc:
            msg = str(exc)
            self.app.after(0, lambda: self._on_error(msg))

    def _on_profile_done(self, schema) -> None:
        self._set_buttons("normal")
        self._banner.show(
            "pass",
            f"\u2018{Path(schema.file).name}\u2019"
            f"   -  {schema.row_count:,} rows, {schema.column_count} columns",
        )
        self._profile_tree.load(schema)
        self._results_nb.select(1)

    # ── Error ──────────────────────────────────────────────────────────────

    def _on_error(self, msg: str) -> None:
        self._set_buttons("normal")
        self._banner.show("fail", msg)


# ---------------------------------------------------------------------------
# Tab 2 — Rules
# ---------------------------------------------------------------------------

class RulesTab(ttk.Frame):
    """View and manage the quality rules saved for each profile."""

    def __init__(self, parent, app: "PipedogApp"):
        super().__init__(parent, padding=14)
        self.app = app
        self._build()

    def _build(self) -> None:
        # ── Profile selector ──────────────────────────────────────────────
        top = ttk.Frame(self)
        top.pack(fill="x", pady=(0, 8))

        ttk.Label(top, text="Profile:", foreground=CLR_DIM).pack(side="left", padx=(0, 6))
        self._profile_var = StringVar()
        self._profile_combo = ttk.Combobox(top, textvariable=self._profile_var,
                                           width=22, state="normal")
        self._profile_combo["values"] = self.app.known_profiles()
        self._profile_combo.pack(side="left", padx=(0, 8))
        self.app._profile_combos.append(self._profile_combo)
        ttk.Button(top, text="Refresh", command=self._load).pack(side="left")

        self._banner = StatusBanner(self)
        self._banner.pack(fill="x", pady=(0, 8))

        # ── Rules table ───────────────────────────────────────────────────
        cols   = ("#", "column", "check_type", "description", "threshold")
        heads  = ("#", "Column", "Check", "Description", "Threshold / Values")
        widths = (32, 130, 120, 300, 130)

        tbl_frame = ttk.Frame(self)
        tbl_frame.pack(fill="both", expand=True, pady=(0, 4))
        tree = ttk.Treeview(tbl_frame, columns=cols, show="headings", height=11,
                            selectmode="extended")
        vsb  = ttk.Scrollbar(tbl_frame, orient="vertical",   command=tree.yview)
        hsb  = ttk.Scrollbar(tbl_frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        for col, head, w in zip(cols, heads, widths):
            tree.heading(col, text=head)
            tree.column(col, width=w, minwidth=28, stretch=(col == "description"))
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        tbl_frame.rowconfigure(0, weight=1)
        tbl_frame.columnconfigure(0, weight=1)
        self._tree = tree

        # ── Row-level actions ─────────────────────────────────────────────
        tbl_btn_row = ttk.Frame(self)
        tbl_btn_row.pack(fill="x", pady=(0, 6))
        self._del_btn = ttk.Button(tbl_btn_row, text="Remove",
                                   command=self._delete_rule, state="disabled")
        self._del_btn.pack(side="left", padx=(0, 6))
        self._edit_btn = ttk.Button(tbl_btn_row, text="Edit",
                                    command=self._edit_rule, state="disabled")
        self._edit_btn.pack(side="left")

        # ── Add a rule ────────────────────────────────────────────────────
        ttk.Separator(self, orient="horizontal").pack(fill="x", pady=(0, 8))
        ttk.Label(self, text="Add a Rule",
                  font=("Segoe UI", 9, "bold"),
                  foreground=CLR_HEADER).pack(anchor="w", pady=(0, 6))

        add = ttk.Frame(self)
        add.pack(fill="x")

        ttk.Label(add, text="Column:").grid(row=0, column=0, sticky="w", padx=(0, 4))
        self._col_var = StringVar()
        self._col_entry = ttk.Entry(add, textvariable=self._col_var, width=16)
        self._col_entry.grid(row=0, column=1, padx=(0, 14))

        ttk.Label(add, text="Check type:").grid(row=0, column=2, sticky="w", padx=(0, 4))
        self._type_var = StringVar()
        self._type_combo = ttk.Combobox(add, textvariable=self._type_var, width=16,
                     values=["not_null", "null_rate", "min_value", "max_value",
                             "unique", "row_count", "allowed_values", "std_dev_change"],
                     state="disabled")
        self._type_combo.grid(row=0, column=3, padx=(0, 14))

        ttk.Label(add, text="Threshold:").grid(row=0, column=4, sticky="w", padx=(0, 4))
        self._thresh_var = StringVar()
        self._thresh_entry = ttk.Entry(add, textvariable=self._thresh_var, width=10)
        self._thresh_entry.grid(row=0, column=5, padx=(0, 12))

        self._add_btn = ttk.Button(add, text="Add Rule", command=self._add_rule)
        self._add_btn.grid(row=0, column=6)

        self._set_rule_controls("disabled")

    def _set_rule_controls(self, state: str) -> None:
        self._col_entry.config(state=state)
        self._type_combo.config(state="readonly" if state == "normal" else "disabled")
        self._thresh_entry.config(state=state)
        self._add_btn.config(state=state)
        self._del_btn.config(state=state)
        self._edit_btn.config(state=state)

    def _auto_refresh(self) -> None:
        profile = self._profile_var.get().strip() or None
        try:
            with self.app.work_dir():
                from .profiler import _pipedog_dir
                exists = (_pipedog_dir(profile) / "schema.json").exists()
        except Exception:
            exists = False
        if exists:
            self._load()
        else:
            self._set_rule_controls("disabled")

    def _load(self) -> None:
        self._tree.delete(*self._tree.get_children())
        self._banner.hide()
        profile = self._profile_var.get().strip() or None
        try:
            with self.app.work_dir():
                from .profiler import load_snapshot
                _, checks = load_snapshot(profile=profile)
            for i, c in enumerate(checks.checks, start=1):
                if c.threshold is not None:
                    thresh = str(c.threshold)
                elif c.expected_value is not None:
                    v = c.expected_value
                    thresh = f"{len(v)} values" if isinstance(v, list) else str(v)
                else:
                    thresh = " -"
                self._tree.insert("", END,
                                   values=(i, c.column, c.check_type, c.description, thresh))
            name = profile or "default"
            self._banner.show("pass",
                              f"\u2018{name}\u2019 has {len(checks.checks)} quality rules")
            self._set_rule_controls("normal")
        except FileNotFoundError:
            self._banner.show("warn",
                "No baseline found for this profile. "
                "Go to Workspace and create one first.")
            self._set_rule_controls("disabled")
        except Exception as exc:
            self._banner.show("fail", str(exc))
            self._set_rule_controls("disabled")

    def _delete_rule(self) -> None:
        sel = self._tree.selection()
        if not sel:
            messagebox.showinfo("Nothing selected", "Click one or more rule rows first.")
            return
        indices = sorted(
            [int(self._tree.item(item, "values")[0]) for item in sel],
            reverse=True,   # delete from the bottom up so indices stay valid
        )
        count = len(indices)
        noun = "rule" if count == 1 else f"{count} rules"
        if not messagebox.askyesno("Remove Rules", f"Remove {noun}? This cannot be undone."):
            return
        profile = self._profile_var.get().strip() or None
        try:
            with self.app.work_dir():
                from .profiler import load_snapshot, CHECKS_FILE, _pipedog_dir
                _, checks = load_snapshot(profile=profile)
                for idx in indices:
                    checks.checks.pop(idx - 1)
                (_pipedog_dir(profile) / CHECKS_FILE).write_text(
                    checks.model_dump_json(indent=2))
            self._banner.show("pass", f"{noun.capitalize()} removed.")
            self._load()
        except Exception as exc:
            self._banner.show("fail", str(exc))

    def _edit_rule(self) -> None:
        sel = self._tree.selection()
        if not sel:
            messagebox.showinfo("Nothing selected", "Click a rule row first.")
            return
        row_values = self._tree.item(sel[0], "values")
        idx        = int(row_values[0])
        col_name   = row_values[1]
        chk_type   = row_values[2]
        cur_desc   = row_values[3]
        cur_thresh = row_values[4] if row_values[4] != " -" else ""

        dlg = tk.Toplevel(self)
        dlg.title(f"Edit Rule #{idx}")
        dlg.resizable(False, False)
        dlg.transient(self.app)
        dlg.grab_set()

        frm = ttk.Frame(dlg, padding=14)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Column", foreground=CLR_DIM).grid(
            row=0, column=0, sticky="w", pady=(0, 2))
        ttk.Label(frm, text=col_name, foreground=CLR_HEADER,
                  font=("Segoe UI", 9, "bold")).grid(row=0, column=1, sticky="w", padx=(8, 0))

        ttk.Label(frm, text="Check type", foreground=CLR_DIM).grid(
            row=1, column=0, sticky="w", pady=(0, 8))
        ttk.Label(frm, text=chk_type, foreground=CLR_HEADER,
                  font=("Segoe UI", 9, "bold")).grid(row=1, column=1, sticky="w", padx=(8, 0))

        ttk.Separator(frm, orient="horizontal").grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=(0, 8))

        ttk.Label(frm, text="Description").grid(row=3, column=0, sticky="w", pady=(0, 4))
        desc_var = StringVar(value=cur_desc)
        ttk.Entry(frm, textvariable=desc_var, width=44).grid(
            row=3, column=1, sticky="ew", padx=(8, 0), pady=(0, 4))

        ttk.Label(frm, text="Threshold").grid(row=4, column=0, sticky="w")
        thresh_var = StringVar(value=cur_thresh)
        ttk.Entry(frm, textvariable=thresh_var, width=18).grid(
            row=4, column=1, sticky="w", padx=(8, 0))
        ttk.Label(frm, text="leave blank to remove",
                  foreground=CLR_HINT,
                  font=("Segoe UI", 8)).grid(row=5, column=1, sticky="w", padx=(8, 0))

        frm.columnconfigure(1, weight=1)

        # Auto-update description when threshold changes
        def _on_thresh_change(*_) -> None:
            new = _rule_description(chk_type, col_name, thresh_var.get())
            if new:
                desc_var.set(new)
        thresh_var.trace_add("write", _on_thresh_change)

        saved = {"ok": False}

        def _ok() -> None:
            saved["ok"] = True
            dlg.destroy()

        btn_row = ttk.Frame(frm)
        btn_row.grid(row=6, column=0, columnspan=2, sticky="e", pady=(14, 0))
        ttk.Button(btn_row, text="Save", command=_ok, width=10).pack(side="left", padx=(0, 6))
        ttk.Button(btn_row, text="Cancel", command=dlg.destroy, width=10).pack(side="left")

        dlg.wait_window()

        if not saved["ok"]:
            return

        new_desc       = desc_var.get().strip()
        new_thresh_raw = thresh_var.get().strip()
        new_thresh: float | None = None
        if new_thresh_raw:
            try:
                new_thresh = float(new_thresh_raw)
            except ValueError:
                messagebox.showwarning("Invalid value", "Threshold must be a number.")
                return

        profile = self._profile_var.get().strip() or None
        try:
            with self.app.work_dir():
                from .profiler import load_snapshot, CHECKS_FILE, _pipedog_dir
                _, checks = load_snapshot(profile=profile)
                rule = checks.checks[idx - 1]
                if new_desc:
                    rule.description = new_desc
                rule.threshold = new_thresh
                (_pipedog_dir(profile) / CHECKS_FILE).write_text(
                    checks.model_dump_json(indent=2))
            self._banner.show("pass", f"Rule #{idx} updated.")
            self._load()
        except Exception as exc:
            self._banner.show("fail", str(exc))

    def _add_rule(self) -> None:
        col   = self._col_var.get().strip()
        ctype = self._type_var.get().strip()
        if not col or not ctype:
            messagebox.showwarning("Missing fields", "Column and check type are required.")
            return
        thresh_raw = self._thresh_var.get().strip()
        threshold: float | None = None
        if thresh_raw:
            try:
                threshold = float(thresh_raw)
            except ValueError:
                messagebox.showwarning("Invalid value", "Threshold must be a number.")
                return
        profile = self._profile_var.get().strip() or None
        try:
            with self.app.work_dir():
                from .profiler import load_snapshot, CHECKS_FILE, _pipedog_dir
                from .schema import QualityCheck
                _, checks = load_snapshot(profile=profile)
                desc = f"Custom: '{col}' {ctype}"
                if threshold is not None:
                    desc += f" (threshold={threshold})"
                checks.checks.append(QualityCheck(
                    column=col, check_type=ctype, description=desc, threshold=threshold))
                (_pipedog_dir(profile) / CHECKS_FILE).write_text(
                    checks.model_dump_json(indent=2))
            self._banner.show("pass", f"Rule added: {desc}")
            self._load()
        except FileNotFoundError:
            self._banner.show("warn",
                "No baseline found. Create one in the Workspace tab first.")
        except Exception as exc:
            self._banner.show("fail", str(exc))


# ---------------------------------------------------------------------------
# Tab 3 — Reports & History
# ---------------------------------------------------------------------------

class ReportsHistoryTab(ttk.Frame):
    """Browse saved reports and the full scan history log."""

    def __init__(self, parent, app: "PipedogApp"):
        super().__init__(parent, padding=14)
        self.app = app
        self._report_paths: list[Path] = []
        self._build()

    def _build(self) -> None:
        top = ttk.Frame(self)
        top.pack(fill="x", pady=(0, 8))

        ttk.Label(top, text="Profile:", foreground=CLR_DIM).pack(side="left", padx=(0, 6))
        self._profile_var = StringVar()
        self._profile_combo = ttk.Combobox(top, textvariable=self._profile_var,
                                           width=22, state="normal")
        self._profile_combo["values"] = self.app.known_profiles()
        self._profile_combo.pack(side="left", padx=(0, 8))
        self.app._profile_combos.append(self._profile_combo)
        ttk.Button(top, text="Refresh", command=self._load_all).pack(side="left")

        self._banner = StatusBanner(self)
        self._banner.pack(fill="x", pady=(0, 8))

        pane = ttk.PanedWindow(self, orient="horizontal")
        pane.pack(fill="both", expand=True)

        # ── Reports ───────────────────────────────────────────────────────
        rep_outer = ttk.Frame(pane, padding=(0, 0, 6, 0))
        pane.add(rep_outer, weight=1)

        ttk.Label(rep_outer, text="Reports",
                  font=("Segoe UI", 9, "bold"),
                  foreground=CLR_HEADER).pack(anchor="w", pady=(0, 4))

        rep_btn_row = ttk.Frame(rep_outer)
        rep_btn_row.pack(fill="x", pady=(0, 4))
        ttk.Button(rep_btn_row, text="Open",
                   command=self._open_selected).pack(side="left", padx=(0, 6))
        ttk.Button(rep_btn_row, text="Open Latest",
                   command=self._open_latest).pack(side="left")

        rep_frame = ttk.Frame(rep_outer)
        rep_frame.pack(fill="both", expand=True)
        rep_tree = ttk.Treeview(rep_frame,
                                columns=("name", "modified"), show="headings", height=14)
        rep_vsb  = ttk.Scrollbar(rep_frame, orient="vertical", command=rep_tree.yview)
        rep_tree.configure(yscrollcommand=rep_vsb.set)
        rep_tree.heading("name",     text="Report File")
        rep_tree.heading("modified", text="Saved")
        rep_tree.column("name",     width=220, minwidth=40, stretch=True)
        rep_tree.column("modified", width=140, minwidth=40, stretch=False)
        rep_tree.grid(row=0, column=0, sticky="nsew")
        rep_vsb.grid(row=0, column=1, sticky="ns")
        rep_frame.rowconfigure(0, weight=1)
        rep_frame.columnconfigure(0, weight=1)
        self._rep_tree = rep_tree

        # ── History ───────────────────────────────────────────────────────
        hist_outer = ttk.Frame(pane, padding=(6, 0, 0, 0))
        pane.add(hist_outer, weight=1)

        ttk.Label(hist_outer, text="History",
                  font=("Segoe UI", 9, "bold"),
                  foreground=CLR_HEADER).pack(anchor="w", pady=(0, 4))
        ttk.Frame(hist_outer, height=28).pack(fill="x")

        hist_frame = ttk.Frame(hist_outer)
        hist_frame.pack(fill="both", expand=True)
        h_cols  = ("timestamp", "file", "result", "checks")
        h_heads = ("When", "File", "Result", "Pass / Warn / Fail")
        h_widths = (140, 160, 60, 120)
        hist_tree = ttk.Treeview(hist_frame, columns=h_cols, show="headings", height=14)
        hist_vsb  = ttk.Scrollbar(hist_frame, orient="vertical", command=hist_tree.yview)
        hist_tree.configure(yscrollcommand=hist_vsb.set)
        for col, head, w in zip(h_cols, h_heads, h_widths):
            hist_tree.heading(col, text=head)
            hist_tree.column(col, width=w, minwidth=40, stretch=(col == "file"))
        hist_tree.tag_configure("pass", foreground=CLR_PASS)
        hist_tree.tag_configure("fail", foreground=CLR_FAIL)
        hist_tree.grid(row=0, column=0, sticky="nsew")
        hist_vsb.grid(row=0, column=1, sticky="ns")
        hist_frame.rowconfigure(0, weight=1)
        hist_frame.columnconfigure(0, weight=1)
        self._hist_tree = hist_tree

    def _auto_refresh(self) -> None:
        self._load_all()

    def _load_all(self) -> None:
        self._load_reports()
        self._load_history()

    def _load_reports(self) -> None:
        self._rep_tree.delete(*self._rep_tree.get_children())
        self._report_paths = []
        profile = self._profile_var.get().strip() or None
        try:
            with self.app.work_dir():
                from .profiler import _pipedog_dir
                reports_dir = (_pipedog_dir(profile) / "reports").resolve()
            if not reports_dir.exists():
                return
            files = sorted(reports_dir.glob("*.xlsx"),
                           key=lambda p: p.stat().st_mtime, reverse=True)
            self._report_paths = files
            for f in files:
                mtime = datetime.fromtimestamp(
                    f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                self._rep_tree.insert("", END, values=(f.name, mtime))
        except Exception:
            pass

    def _load_history(self) -> None:
        self._hist_tree.delete(*self._hist_tree.get_children())
        profile = self._profile_var.get().strip() or None
        try:
            with self.app.work_dir():
                from .history import load_history
                history = load_history(profile)
            for e in reversed(history.entries):
                ts     = e.timestamp[:19].replace("T", " ")
                fname  = Path(e.file_scanned).name
                result = "Pass" if e.overall_passed else "Fail"
                checks = f"{e.passed_count}  /  {e.warning_count}  /  {e.failed_count}"
                tag    = "pass" if e.overall_passed else "fail"
                self._hist_tree.insert("", END,
                                        values=(ts, fname, result, checks),
                                        tags=(tag,))
            count = len(history.entries)
            name  = profile or "default"
            self._banner.show(
                "pass" if count else "warn",
                f"\u2018{name}\u2019   -  "
                f"{count} scan(s) in history,  {len(self._report_paths)} saved report(s)",
            )
        except Exception:
            if self._report_paths:
                self._banner.show("pass",
                                  f"{len(self._report_paths)} report(s)   -  no scan history yet")

    def _open_selected(self) -> None:
        sel = self._rep_tree.selection()
        if not sel:
            messagebox.showinfo("Nothing selected", "Click a report row first.")
            return
        idx = self._rep_tree.index(sel[0])
        if 0 <= idx < len(self._report_paths):
            _open_file(self._report_paths[idx].resolve())

    def _open_latest(self) -> None:
        profile = self._profile_var.get().strip() or None
        try:
            with self.app.work_dir():
                from .reporter import open_last_report
                open_last_report(profile)
        except Exception as exc:
            messagebox.showerror("Error", str(exc))


# ---------------------------------------------------------------------------
# Main application window
# ---------------------------------------------------------------------------

class PipedogApp(Tk):

    def __init__(self):
        super().__init__()
        self.title("\U0001f436 Pipedog  - Data Quality Monitor")
        self.geometry("980x680")
        self.minsize(720, 500)
        self.configure(background=CLR_BG)

        self._work_dir = StringVar(value=self._load_default_dir())
        self._profile_combos: list[ttk.Combobox] = []

        self._apply_theme()
        self._build_header()
        self._build_notebook()
        self._build_statusbar()

    # ── Theme ──────────────────────────────────────────────────────────────

    def _apply_theme(self) -> None:
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure(".", background=CLR_BG, foreground=CLR_HEADER, font=("Segoe UI", 9))
        s.configure("TFrame",       background=CLR_BG)
        s.configure("TLabel",       background=CLR_BG)
        s.configure("TCheckbutton", background=CLR_BG)
        s.configure("TNotebook",    background=CLR_BG, tabmargins=[2, 4, 2, 0])
        s.configure("TNotebook.Tab", padding=[14, 5], font=("Segoe UI", 9))
        s.map("TNotebook.Tab",
              background=[("selected", CLR_PANEL)],
              foreground=[("selected", CLR_HEADER)])
        s.configure("TButton",      padding=[8, 4])
        s.configure("TPanedwindow", background=CLR_BG)
        s.configure("Treeview",     background=CLR_PANEL,
                    fieldbackground=CLR_PANEL, rowheight=22)
        s.configure("Treeview.Heading", font=("Segoe UI", 9, "bold"),
                    background="#f3f4f6", foreground=CLR_HEADER)
        s.configure("TSeparator",   background="#e5e7eb")

    # ── Header ─────────────────────────────────────────────────────────────

    def _build_header(self) -> None:
        hdr = ttk.Frame(self, padding=(14, 8, 14, 4))
        hdr.pack(fill="x")

        tk.Label(hdr, text="\U0001f436 Pipedog",
                 font=("Segoe UI Emoji", 15, "bold"),
                 background=CLR_BG, foreground=CLR_HEADER).grid(
            row=0, column=0, sticky="w")
        tk.Label(hdr, text="Data quality and drift detection",
                 font=("Segoe UI", 9),
                 background=CLR_BG, foreground=CLR_DIM).grid(
            row=1, column=0, sticky="w")

        wd_frame = ttk.Frame(hdr)
        wd_frame.grid(row=0, column=1, rowspan=2, sticky="e", padx=(16, 0))
        hdr.columnconfigure(1, weight=1)

        ttk.Label(wd_frame, text="Project folder:",
                  foreground=CLR_DIM).pack(side="left", padx=(0, 6))
        self._wd_label = ttk.Label(wd_frame, foreground=CLR_DIM, width=42, anchor="e")
        self._wd_label.pack(side="left", padx=(0, 6))
        self._work_dir.trace_add("write", self._update_wd_label)
        self._update_wd_label()
        ttk.Button(wd_frame, text="Change…",
                   command=self._change_work_dir).pack(side="left")

        ttk.Separator(self, orient="horizontal").pack(fill="x")

    # ── Notebook ───────────────────────────────────────────────────────────

    def _build_notebook(self) -> None:
        nb = ttk.Notebook(self, padding=(10, 8, 10, 0))
        nb.pack(fill="both", expand=True)

        self._tabs = [
            WorkspaceTab(nb, self),
            RulesTab(nb, self),
            ReportsHistoryTab(nb, self),
        ]
        labels = ["Workspace", "Rules", "Reports & History"]
        for frame, label in zip(self._tabs, labels):
            nb.add(frame, text=label)

        self._notebook = nb
        nb.bind("<<NotebookTabChanged>>", self._on_tab_changed)

    def _on_tab_changed(self, event) -> None:
        idx = self._notebook.index(self._notebook.select())
        tab = self._tabs[idx]
        if hasattr(tab, "_auto_refresh"):
            tab._auto_refresh()

    # ── Status bar ─────────────────────────────────────────────────────────

    def _build_statusbar(self) -> None:
        tk.Frame(self, background="#e5e7eb", height=1).pack(fill="x", side="bottom")
        self._status_lbl = tk.Label(
            self, text="Choose a project folder, pick a file, and run a check.",
            background=CLR_BG, foreground=CLR_DIM,
            font=("Segoe UI", 8), anchor="w", padx=12, pady=3,
        )
        self._status_lbl.pack(fill="x", side="bottom")

    # ── Profile helpers ────────────────────────────────────────────────────

    def known_profiles(self) -> list[str]:
        profiles: set[str] = set(_known_profiles(self._work_dir.get()))
        try:
            data = json.loads(CONFIG_FILE.read_text())
            for p in data.get("known_profiles", []):
                if p:
                    profiles.add(p)
        except Exception:
            pass
        return sorted(profiles)

    def remember_profile(self, profile: str | None) -> None:
        name = (profile or "").strip() or "default"
        try:
            existing: dict = {}
            if CONFIG_FILE.exists():
                existing = json.loads(CONFIG_FILE.read_text())
            saved: list[str] = existing.get("known_profiles", [])
            if name not in saved:
                saved.append(name)
                existing["known_profiles"] = saved
                CONFIG_FILE.write_text(json.dumps(existing, indent=2))
        except Exception:
            pass
        self.refresh_profile_combos()

    def refresh_profile_combos(self) -> None:
        profiles = self.known_profiles()
        for combo in self._profile_combos:
            try:
                combo["values"] = profiles
            except Exception:
                pass

    # ── Config helpers ─────────────────────────────────────────────────────

    def _load_default_dir(self) -> str:
        try:
            data  = json.loads(CONFIG_FILE.read_text())
            saved = data.get("default_working_dir", "")
            if saved and Path(saved).is_dir():
                return saved
        except Exception:
            pass
        return str(Path.cwd())

    def _save_default_dir(self, path: str) -> None:
        try:
            existing: dict = {}
            if CONFIG_FILE.exists():
                existing = json.loads(CONFIG_FILE.read_text())
            existing["default_working_dir"] = path
            CONFIG_FILE.write_text(json.dumps(existing, indent=2))
        except Exception:
            pass

    def _update_wd_label(self, *_) -> None:
        self._wd_label.config(text=_short(self._work_dir.get(), 42))

    def _change_work_dir(self) -> None:
        path = _pick_dir("Select the folder where .pipedog/ will be stored")
        if path:
            self._work_dir.set(path)
            self._save_default_dir(path)
            self._status_lbl.config(text=f"Project folder set to: {path}")
            self.refresh_profile_combos()

    def work_dir(self):
        return _WorkDir(self._work_dir.get())


# ---------------------------------------------------------------------------
# Working directory context manager
# ---------------------------------------------------------------------------

class _WorkDir:
    def __init__(self, path: str):
        self._target = path
        self._orig   = os.getcwd()

    def __enter__(self):
        os.chdir(self._target)
        return self

    def __exit__(self, *_):
        os.chdir(self._orig)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    app = PipedogApp()
    app.mainloop()


if __name__ == "__main__":
    main()
