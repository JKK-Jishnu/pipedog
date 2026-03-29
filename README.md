# Pipedog

An open source data quality and schema drift detection tool for analysts and data engineers. Point it at a CSV, Parquet, or JSON file and it will profile the data, auto-generate quality checks, and alert you the moment something changes.

---

## Why Pipedog?

Data pipelines break silently. A column gets renamed upstream, nulls creep into a field that was always clean, a price column suddenly contains strings. These issues reach production before anyone notices.

Pipedog solves this by:
- **Taking a snapshot** of your data's structure and statistics on day one.
- **Scanning every new file** against that snapshot and failing loudly when something drifts.
- **Explaining what went wrong** in plain English, not stack traces.

---

## Installation

### With pip (quickest)

```bash
pip install pipedog
```

### With Poetry (for development)

```bash
git clone https://github.com/JKK-Jishnu/pipedog.git
cd pipedog
poetry install
```

### Dependencies

| Package   | Purpose                              |
|-----------|--------------------------------------|
| typer     | CLI framework                        |
| rich      | Coloured terminal output             |
| pandas    | File reading (CSV, Parquet, JSON)    |
| pyarrow   | Parquet support for pandas           |
| duckdb    | SQL engine (reserved for future use) |
| pydantic  | Schema validation and JSON I/O       |

---

## Quick Start

```bash
# 1. Profile your file and save a baseline snapshot
pipedog init data/orders.csv

# 2. Tomorrow, when a new file arrives, scan it
pipedog scan data/orders_new.csv

# 3. Explore any file without saving anything
pipedog profile data/orders.csv
```

---

## Commands

### `pipedog init <file>`

Profiles the file and saves two files to `.pipedog/`:

- **`.pipedog/schema.json`** — column names, types, null stats, value ranges, timestamps.
- **`.pipedog/checks.json`** — auto-generated quality rules derived from the baseline.

```
pipedog init sample_data/orders.csv
```

**What gets auto-generated:**

| Rule        | When generated                              | Severity |
|-------------|---------------------------------------------|----------|
| `not_null`  | Column had zero nulls at init time          | error    |
| `null_rate` | Column had some nulls; threshold = pct + 10 | warning  |
| `min_value` | Numeric column; locks in the observed min   | error    |
| `max_value` | Numeric column; locks in the observed max   | error    |
| `unique`    | Every value was distinct (looks like a key) | error    |

Re-running `init` refreshes the baseline to the current file.

---

### `pipedog scan <file>`

Compares the file against the baseline and runs all quality checks.

```
pipedog scan sample_data/orders.csv
```

**Exit codes:**
- `0` — all checks passed (warnings are allowed).
- `1` — one or more error-severity checks failed.

This makes `pipedog scan` CI/CD friendly — pipe it into your build and it will fail the pipeline when data quality breaks.

**What gets checked:**

1. **Schema drift** — were columns added, removed, or changed type?
2. **Quality checks** — do null rates, value ranges, and uniqueness still match the baseline?

**Output example (all passing):**
```
+------------------------------- Pipedog Scan --------------------------------+
| ALL CHECKS PASSED                                                           |
| 10 rows · 7 columns · 17 passed · 0 warnings · 0 failed                    |
+-----------------------------------------------------------------------------+

Passed Checks
  PASS  No nulls found in 'order_id'.
  PASS  'price' maximum is 149.99, within baseline maximum of 149.99.
  ...
```

**Output example (failure):**
```
+------------------------------- Pipedog Scan --------------------------------+
| CHECKS FAILED                                                               |
| 12 rows · 6 columns · 14 passed · 0 warnings · 2 failed                    |
+-----------------------------------------------------------------------------+

Schema Drift Detected
  FAIL  Column 'status' existed in the baseline but is missing from the current file.

Failed Checks
  FAIL  'order_id' has 2 null value(s) (16.67% of rows).
```

---

### `pipedog profile <file>`

Shows a data summary without saving anything to disk. Useful for exploring a file before committing to a baseline.

```
pipedog profile sample_data/orders.csv
```

**Output includes:**
- Total row and column count.
- Per-column type, null count, null percentage, unique count.
- Min and max for numeric columns.
- Up to 3 sample values per column.

---

## Supported File Types

| Extension        | Format  |
|------------------|---------|
| `.csv`           | CSV     |
| `.parquet` `.pq` | Parquet |
| `.json`          | JSON    |

File type is detected automatically from the extension.

---

## How It Works

```
pipedog init orders.csv
    │
    ├─ load_file()          reads CSV/Parquet/JSON into a DataFrame
    ├─ profile_dataframe()  computes stats for every column
    ├─ generate_checks()    auto-generates quality rules from the stats
    └─ save_snapshot()      writes .pipedog/schema.json + checks.json

pipedog scan orders_new.csv
    │
    ├─ load_file()          reads the new file
    ├─ load_snapshot()      loads baseline from .pipedog/
    ├─ profile_dataframe()  profiles the new file
    ├─ detect_drift()       compares column structure
    ├─ run_quality_checks() evaluates every rule
    └─ print_scan_results() renders colour-coded report, returns exit code
```

---

## Project Structure

```
pipedog/
├── pyproject.toml          # Poetry config and PyPI metadata
├── README.md               # This file
├── sample_data/
│   └── orders.csv          # Example file to test with
└── pipedog/
    ├── __init__.py         # Package version
    ├── main.py             # CLI commands (init, scan, profile)
    ├── schema.py           # Pydantic models (ColumnSchema, DataSchema, etc.)
    ├── profiler.py         # File loading, type inference, statistical profiling
    ├── scanner.py          # Drift detection and quality check evaluation
    └── output.py           # Rich terminal output (tables, panels, colours)
```

---

## Snapshot Files

After running `pipedog init`, a `.pipedog/` directory is created:

```
.pipedog/
├── schema.json    # baseline column statistics
└── checks.json    # auto-generated quality rules
```

These files are plain JSON and human-readable. You can commit them to version control to track schema changes over time, or add `.pipedog/` to `.gitignore` to keep them local.

**Example `.pipedog/schema.json`:**
```json
{
  "file": "/data/orders.csv",
  "row_count": 10,
  "column_count": 7,
  "columns": [
    {
      "name": "order_id",
      "dtype": "integer",
      "nullable": false,
      "null_count": 0,
      "null_pct": 0.0,
      "unique_count": 10,
      "sample_values": [1, 2, 3],
      "min_value": 1.0,
      "max_value": 10.0,
      "mean_value": 5.5
    }
  ],
  "captured_at": "2026-03-26T18:34:20.123456+00:00"
}
```

---

## CI/CD Integration

Because `pipedog scan` exits with code `1` on failure, it drops straight into any CI pipeline:

**GitHub Actions:**
```yaml
- name: Check data quality
  run: pipedog scan data/daily_export.csv
```

**Makefile:**
```makefile
check:
    pipedog scan data/daily_export.csv
```

---

## Roadmap

- [ ] `pipedog diff` — side-by-side comparison of two snapshots
- [ ] Custom checks via `checks.json` (regex patterns, allowed value sets)
- [ ] JSON Lines (`.jsonl`) support
- [ ] `--output json` flag for machine-readable scan results
- [ ] Excel (`.xlsx`) support
- [ ] Slack / webhook notifications on failure

---

## License

MIT
