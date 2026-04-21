# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

# Collect data files, binaries, and hidden imports for packages that need it
datas, binaries, hiddenimports = [], [], []
for pkg in ["pyarrow", "duckdb", "openpyxl", "pydantic"]:
    d, b, h = collect_all(pkg)
    datas += d
    binaries += b
    hiddenimports += h

a = Analysis(
    ["pipedog_launcher.py"],
    pathex=["."],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports + [
        # pipedog submodules (imported dynamically inside workers)
        "pipedog",
        "pipedog.profiler",
        "pipedog.scanner",
        "pipedog.reporter",
        "pipedog.history",
        "pipedog.schema",
        "pipedog.output",
        # Excel
        "openpyxl",
        "openpyxl.styles",
        "openpyxl.utils",
        "openpyxl.workbook",
        "openpyxl.worksheet",
        "pyxlsb",
        # Arrow
        "pyarrow.vendored.version",
        # Pydantic extras
        "pydantic.deprecated.class_validators",
        # GUI
        "tkinter",
        "tkinter.ttk",
        "tkinter.filedialog",
        "tkinter.messagebox",
        # Rich / Typer (used by CLI modules imported transitively)
        "rich",
        "typer",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "matplotlib", "scipy", "IPython", "notebook",
        "pytest", "sphinx", "docutils",
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

# Single-file build — everything packed into one Pipedog.exe
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="Pipedog",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,   # no black terminal window behind the GUI
    windowed=True,
    onefile=True,
)
