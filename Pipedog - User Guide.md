# Pipedog - User Guide
**Version 0.5.0**

---

## What is Pipedog?

Pipedog is a desktop tool that monitors your data files (CSV, Excel, JSON) for quality issues and unexpected changes. You point it at a file, it learns the normal pattern, and then every time a new version of that file arrives it tells you exactly what changed or went wrong.

No installation required. No Python. Just double-click **Pipedog.exe** and start.

---

## Getting Started

### Step 1 - Set your project folder

When the app opens, look at the top-right corner. You will see **"Project folder"** with a path shown next to it.

- Click **Change…** to pick the folder where your data files live
- Pipedog will store its records inside a `.pipedog` subfolder there
- This setting is remembered the next time you open the app

---

## The Three Tabs

---

### Tab 1 - Workspace

This is where you do all the main work.

**How to use:**

1. Click **Browse…** and select your data file (CSV, Excel, JSON, Parquet)
2. If the file is an Excel workbook with multiple sheets, a dialog will ask you which sheet to read
3. If your data does not start on row 1 (e.g. there are titles or blank rows above the column headers), change the **"Data starts at row"** number to the correct row
4. Optionally enter a **Profile** name (e.g. `sales`, `orders`) - this lets you manage multiple file types separately. Leave blank to use the default
5. Click one of the three action buttons:

| Button | What it does |
|--------|-------------|
| **Create Baseline** | Reads the file and saves a snapshot of what "normal" looks like. Do this once with your best reference file. |
| **Run Scan** | Compares the file against the saved baseline. Shows what changed, what failed, and what passed. |
| **Explore Data** | Shows column statistics for any file without saving anything. Good for exploring an unfamiliar file. |

**After a scan:**
- The result banner turns **green** (all passed), **yellow** (warnings), or **red** (failures)
- The **Scan Results** tab shows every check with Pass / Warn / Fail
- The **Data Profile** tab shows column statistics. Rows highlighted in red have missing (null) values
- Click **Open Report** to open the full Excel report in Excel

---

### Tab 2 - Rules

Shows all the quality rules that were automatically created when you ran "Create Baseline".

**Things you can do here:**

- Select a **Profile** from the dropdown and click **Refresh** to load its rules
- **Edit a rule** - click a row, then click **Edit**. You can change the threshold or description. The description updates automatically when you change the threshold
- **Remove rules** - click a row (or hold Ctrl and click multiple rows) then click **Remove**. You can delete many rules at once
- **Add a rule** - use the form at the bottom. Pick the column, check type, and an optional threshold, then click **Add Rule**

**Check types explained:**

| Check type | What it checks |
|------------|---------------|
| `not_null` | Column must have no missing values |
| `null_rate` | Missing value % must stay below a threshold |
| `min_value` | Column values must not go below a minimum |
| `max_value` | Column values must not exceed a maximum |
| `unique` | All values must be unique (ID/key columns) |
| `allowed_values` | Only known category values are allowed (e.g. status must be pending/shipped/delivered) |
| `row_count` | File must have at least a minimum number of rows |
| `std_dev_change` | Distribution must not shift drastically from baseline |

---

### Tab 3 - Reports & History

**Reports panel (left side)**
- Lists all saved Excel reports for the selected profile
- Double-click or click **Open** to open a report in Excel
- Click **Open Latest** to open the most recent report

**History panel (right side)**
- Shows every scan that has been run - timestamp, file name, result, and pass/warn/fail counts
- Most recent scans appear at the top

---

## Typical Workflow

```
Month 1 - New file arrives
  1. Open Pipedog
  2. Set project folder
  3. Browse to the file
  4. Click "Create Baseline"
  5. Done - baseline saved

Month 2 - Next version of the file arrives
  1. Browse to the new file
  2. Click "Run Scan"
  3. Review results - green means clean, red means something changed
  4. Click "Open Report" to get the full Excel report
  5. Share the report with your team
```

---

## Understanding Scan Results

| Colour | Meaning |
|--------|---------|
| Green (Pass) | Check passed - value is within expected range |
| Yellow (Warn) | Something changed but not critical - worth reviewing |
| Red (Fail) | Check failed - data has drifted outside the expected range |
| Purple (Drift) | Column structure changed - a column was added, removed, or its type changed |

---

## Excel Report Structure

Each scan saves an Excel report with three sheets:

| Sheet | Contents |
|-------|----------|
| **Summary** | Overall result, file name, scan time, baseline date, check counts |
| **Results** | Full table of all checks - colour coded, filterable |
| **Profile** | Column-by-column statistics of the scanned file |

---

## Tips

- You can manage multiple file types by using different **Profile** names. For example, use profile `sales` for your sales file and profile `gst` for your tax file. Each profile has its own baseline, rules, and history
- To share a baseline with a colleague, send them the `.pipedog/` folder from your project folder. They place it in the same folder on their machine and Pipedog will pick it up
- The **"Data starts at row"** setting is useful for Excel files that have company headers, titles, or blank rows above the actual column names

---

## Supported File Types

| Extension | Format |
|-----------|--------|
| `.csv` | Comma-separated values |
| `.xlsx` | Excel workbook |
| `.xlsb` | Excel Binary workbook |
| `.parquet` / `.pq` | Parquet (big data format) |
| `.json` | JSON (array or records format) |

---

*Built with Pipedog v0.5.0*
