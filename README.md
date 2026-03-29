# Pipedog

**v0.2.0** — Open source data quality and schema drift detection tool for analysts and data engineers.

Point it at a CSV, Parquet, or JSON file and it will profile the data, auto-generate quality checks, and alert you the moment something changes — with a colour-coded terminal report and a saved HTML report you can open in any browser.

---

## Why Pipedog?

Data pipelines break silently. A column gets renamed upstream, nulls creep into a field that was always clean, a price column suddenly contains strings, a monthly file arrives half-empty. These issues reach production before anyone notices.

Pipedog solves this by:
- **Taking a snapshot** of your data's structure and statistics on day one.
- **Scanning every new file** against that snapshot and failing loudly when something drifts.
- **Explaining what went wrong** in plain English, not stack traces.
- **Saving an HTML report** after every scan — attach it to an email, open it in a browser, share it with your team.

---

## Installation

```bash
pip install pipedog
```

### Development setup

```bash
git clone https://github.com/JKK-Jishnu/pipedog.git
cd pipedog
poetry install
```

### Dependencies

| Package  | Purpose                                      |
|----------|----------------------------------------------|
| typer    | CLI framework                                |
| rich     | Colour-coded terminal output                 |
| pandas   | File reading (CSV, Parquet, JSON)            |
| pyarrow  | Parquet support for pandas                   |
| duckdb   | SQL engine (reserved for future use)         |
| pydantic | Schema validation and JSON I/O               |

---

## Quick Start

```bash
# 1. Profile your file and save a baseline snapshot
pipedog init data/orders.csv

# 2. Next month, when a new file arrives, scan it
pipedog scan data/orders_feb.csv

# 3. Explore any file without saving anything
pipedog profile data/orders.csv
```

---

## Commands

### `pipedog init <file> [<file2> ...] [--profile <name>]`

Profiles one or more files and saves a baseline snapshot to `.pipedog/`.

```bash
# Single file
pipedog init orders.csv

# Named profile — stores snapshot in .pipedog/purchase/
pipedog init purchase_jan.csv --profile purchase

# Multi-file baseline — merges stats from all three months
pipedog init sales_jan.csv sales_feb.csv sales_mar.csv --profile sales
```

**What gets saved:**

| File | Contents |
|------|----------|
| `.pipedog/<profile>/schema.json` | Column names, types, null stats, value ranges, distribution stats, allowed values |
| `.pipedog/<profile>/checks.json` | Auto-generated quality rules |
| `.pipedog/<profile>/reports/`    | HTML scan reports (created on first scan) |

**Auto-generated quality rules:**

| Rule            | When generated                                        | Severity |
|-----------------|-------------------------------------------------------|----------|
| `not_null`      | Column had zero nulls at init time                    | error    |
| `null_rate`     | Column had some nulls; threshold = baseline % + 10pp  | warning  |
| `min_value`     | Numeric column; locks observed minimum                | error    |
| `max_value`     | Numeric column; locks observed maximum                | error    |
| `unique`        | Every value was distinct (key column detection)       | error    |
| `allowed_values`| String/boolean column with <= 50 distinct values      | error    |
| `std_dev_change`| Numeric column; flags distribution shift > 50%        | warning  |
| `row_count`     | Every file; threshold = 80% of baseline row count avg | error    |

**Multi-file baseline:**
When you pass multiple files, Pipedog merges their statistics into one smart baseline:
- Global min/max across all files
- Weighted average null rates
- Union of allowed values (for category columns)
- Average row count used as the row_count check threshold
- All files must have identical column names and types (clear error if not)

Re-running `init` on the same profile overwrites the existing baseline.

---

### `pipedog scan <file> [--profile <name>] [--no-report]`

Compares the file against the saved baseline and runs all quality checks.

```bash
pipedog scan orders_feb.csv
pipedog scan purchase_feb.csv --profile purchase
pipedog scan data.csv --profile sales --no-report
```

**What gets checked:**
1. **Schema drift** — columns added, removed, or type changed
2. **Quality checks** — null rates, value ranges, row count, new categories, distribution shift

**After every scan:**
- An HTML report is saved to `.pipedog/<profile>/reports/<file>-<profile>-<timestamp>.html`
- One entry is appended to `.pipedog/<profile>/history.json` (audit trail)

**Exit codes:**
- `0` — all checks passed (warnings allowed)
- `1` — one or more error-severity checks failed

**Output example — all passing:**
```
+------------------------------- Pipedog Scan --------------------------------+
| ALL CHECKS PASSED                                                           |
| 10 rows · 7 columns · 24 passed · 0 warnings · 0 failed                    |
+-----------------------------------------------------------------------------+

Passed Checks
  PASS  No nulls found in 'order_id'.
  PASS  'status' contains only known values.
  PASS  Row count is 10, meets minimum of 8.
  PASS  'price' std deviation changed 0.0% (baseline=46.4, current=46.4).
  ...

HTML report saved: .pipedog/reports/orders-default-20260329-071607.html
```

**Output example — failure:**
```
+------------------------------- Pipedog Scan --------------------------------+
| CHECKS FAILED                                                               |
| 8 rows · 6 columns · 14 passed · 0 warnings · 3 failed                     |
+-----------------------------------------------------------------------------+

Schema Drift Detected
  FAIL  Column 'status' existed in the baseline but is missing.

Failed Checks
  FAIL  'order_id' has 2 null value(s) (25.0% of rows).
  FAIL  Row count is 8, below minimum of 8 (80% of baseline average).
```

---

### `pipedog profile <file>`

Shows a data summary without saving anything to disk.

```bash
pipedog profile orders.csv
```

Displays: row count, column types, null counts, null %, unique counts, min/max, sample values. Nothing is written to disk — useful for exploring a file before committing to a baseline.

---

### `pipedog checks list [--profile <name>]`

Shows all quality rules for a profile in a readable table.

```bash
pipedog checks list
pipedog checks list --profile purchase
```

---

### `pipedog checks add --column <col> --type <type> [--threshold <n>] [--profile <name>]`

Add a custom quality rule that wasn't auto-generated.

```bash
# Enforce a custom price ceiling
pipedog checks add --column price --type max_value --threshold 9999

# Enforce not-null on a column that happened to have nulls at init time
pipedog checks add --column customer_id --type not_null --profile purchase
```

---

### `pipedog checks edit [--profile <name>]`

Opens `checks.json` directly in your `$EDITOR` (or prints the file path if `$EDITOR` is not set). Changes take effect on the next `pipedog scan`.

```bash
pipedog checks edit
pipedog checks edit --profile purchase
```

---

### `pipedog report [--profile <name>] [--last]`

List available HTML reports or open the most recent one in your browser.

```bash
# List all reports for the default profile
pipedog report

# Open the most recent report in the browser
pipedog report --last

# Open the most recent report for a specific profile
pipedog report --last --profile purchase
```

---

## Multi-Profile Workflow

For projects with multiple file types (e.g. a monthly purchase CSV + a GST JSON file):

```bash
# One-time setup — use your most complete historical month
pipedog init purchase_jan.csv --profile purchase
pipedog init gstr1_jan.json   --profile gstr1

# Every month when new files arrive
pipedog scan purchase_feb.csv --profile purchase   # exit 0 or 1
pipedog scan gstr1_feb.json   --profile gstr1      # exit 0 or 1
```

Each profile stores its own independent snapshot, checks, history, and reports.

---

## .pipedog Directory Layout

```
.pipedog/
├── schema.json                    # default profile snapshot
├── checks.json                    # default profile rules
├── history.json                   # default profile audit log
├── reports/
│   └── orders-default-20260329-071607.html
│
├── purchase/                      # --profile purchase
│   ├── schema.json
│   ├── checks.json
│   ├── history.json
│   └── reports/
│       └── purchase_feb-purchase-20260329-143022.html
│
└── gstr1/                         # --profile gstr1
    ├── schema.json
    ├── checks.json
    └── history.json
```

Commit `.pipedog/` to version control to track schema changes over time, or add it to `.gitignore` to keep it local.

---

## Supported File Types

| Extension         | Format  |
|-------------------|---------|
| `.csv`            | CSV     |
| `.parquet` `.pq`  | Parquet |
| `.json`           | JSON    |

File type is detected automatically from the extension.

---

## How It Works

```
pipedog init jan.csv feb.csv --profile sales
    │
    ├─ load_file()          reads each file into a DataFrame
    ├─ profile_dataframe()  computes stats per column (nulls, ranges,
    │                       distribution, allowed values)
    ├─ merge_schemas()      merges multi-file stats into one baseline
    ├─ generate_checks()    auto-generates quality rules
    └─ save_snapshot()      writes .pipedog/sales/schema.json + checks.json

pipedog scan feb.csv --profile sales
    │
    ├─ load_file()           reads the new file
    ├─ load_snapshot()       loads baseline from .pipedog/sales/
    ├─ profile_dataframe()   profiles the new file
    ├─ detect_drift()        compares column structure (names, types)
    ├─ run_quality_checks()  evaluates all 8 check types
    ├─ generate_html_report() builds the HTML report
    ├─ save_report()         saves HTML to .pipedog/sales/reports/
    ├─ append_scan_result()  appends entry to history.json
    └─ print_scan_results()  renders colour-coded terminal output
```

---

## Project Structure

```
pipedog/
├── pyproject.toml          # Poetry config and PyPI metadata
├── README.md               # This file
├── LICENSE                 # MIT
├── sample_data/
│   └── orders.csv          # Example file to test with
└── pipedog/
    ├── __init__.py         # Package version
    ├── main.py             # All CLI commands and sub-commands
    ├── schema.py           # Pydantic models (ColumnSchema, DataSchema,
    │                       #   QualityCheck, CheckResult, ScanHistory, etc.)
    ├── profiler.py         # File loading, type inference, stats, snapshot I/O,
    │                       #   multi-file merge
    ├── scanner.py          # Drift detection and quality check evaluation
    ├── output.py           # Rich terminal rendering (tables, panels, colours)
    ├── reporter.py         # HTML report generation and file I/O
    └── history.py          # Scan history persistence (history.json)
```

---

## schema.json Format (v0.2.0)

```json
{
  "file": "/data/orders.csv",
  "row_count": 10,
  "column_count": 7,
  "captured_at": "2026-03-29T07:15:59+00:00",
  "source_files": ["/data/orders.csv"],
  "row_count_mean": 10.0,
  "row_count_std": 0.0,
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
      "mean_value": 5.5,
      "std_dev": 3.0277,
      "p25": 3.25,
      "p50": 5.5,
      "p75": 7.75,
      "allowed_values": null,
      "all_unique": true
    },
    {
      "name": "status",
      "dtype": "string",
      "nullable": false,
      "null_count": 0,
      "null_pct": 0.0,
      "unique_count": 3,
      "sample_values": ["shipped", "pending", "delivered"],
      "allowed_values": ["delivered", "pending", "shipped"],
      "all_unique": false
    }
  ]
}
```

---

## CI/CD Integration

`pipedog scan` exits with code `1` on failure — works natively in any CI pipeline.

**GitHub Actions:**
```yaml
- name: Validate data quality
  run: |
    pipedog scan data/daily_export.csv --profile daily
    # Fails the build if checks fail — exit code 1
```

**Gate before loading to database:**
```bash
pipedog scan staging/export.parquet --profile production
if [ $? -eq 1 ]; then
    echo "Data quality failed — not loading to DB"
    exit 1
fi
python load_to_database.py staging/export.parquet
```

**Monthly batch (multiple files):**
```bash
for file in data/2026_*.csv; do
    pipedog scan "$file" --profile monthly || exit 1
done
```

---

## Roadmap

- [ ] `pipedog history` — show null rate trends over time from history.json
- [ ] `pipedog diff` — side-by-side comparison of two snapshots
- [ ] JSON Lines (`.jsonl`) support
- [ ] Excel (`.xlsx`) support
- [ ] `--output json` flag for machine-readable scan results
- [ ] `pipedogfin` — finance/tax/compliance focused package (GST validation, invoice format checks, accounting API connectors)

---

## License

MIT
