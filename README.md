# YYY Holdings Tracker

![YYY Holdings Tracker](assets/hero.svg)

[![Daily holdings fetch](https://github.com/ikgeorgiev/yyy_holdings/actions/workflows/daily_fetch.yml/badge.svg)](https://github.com/ikgeorgiev/yyy_holdings/actions/workflows/daily_fetch.yml)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
![Python](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white)
![Streamlit](https://img.shields.io/badge/streamlit-app-FF4B4B?logo=streamlit&logoColor=white)

Track, store, and visualize daily holdings for the Amplify YYY ETF. This repo includes a scheduled ingestion job, a DuckDB data store, and a Streamlit dashboard to compare holdings across dates.

## At a Glance
- Data source: Amplify YYY holdings page and CSV feed.
- Update cadence: daily GitHub Action, commits `holdings.duckdb`.
- Storage: DuckDB table with `date`, `ticker`, `name`, `shares`, `market_value`, `weight`.
- Stack: Python, pandas, DuckDB, Streamlit, Plotly.
- UI: compare dates, added/removed/changed positions, totals and deltas.

## Preview
![Dashboard preview](assets/dashboard-preview.svg)

Tip: Replace the mock with a real screenshot by exporting the Streamlit page to PNG.

## Architecture
```mermaid
flowchart LR
  A[Amplify holdings page / CSV] --> B[ingest.py]
  B --> C[(holdings.duckdb)]
  D[backfill_excel.py] --> C
  E[GitHub Action] --> B
  C --> F[app.py (Streamlit)]
```

## Quickstart
Prereqs: Python 3.11+ recommended.

Windows:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

macOS/Linux:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage
Run the dashboard:
```powershell
streamlit run app.py
```

Fetch the latest holdings:
```powershell
python ingest.py
```

Backfill historical files:
```powershell
python backfill_excel.py <path-to-file-or-folder> --recursive
```

Work with a local DB (avoid modifying the tracked file):
```powershell
python ingest.py --db holdings.local.duckdb
```

## Project Structure
- `ingest.py` - fetches holdings and upserts into DuckDB.
- `compare.py` - query helpers used by the app.
- `app.py` - Streamlit dashboard.
- `backfill_excel.py` - imports historical CSV/Excel files.
- `holdings.duckdb` - tracked datastore updated by CI.
- `.github/workflows/daily_fetch.yml` - scheduled ingest job.

## Data and Automation
- The workflow in `.github/workflows/daily_fetch.yml` runs daily and commits updates to `holdings.duckdb`.
- Update timing depends on upstream publication time.

## Contributing
See `AGENTS.md` for contributor guidelines, style, and workflow notes.

## Disclaimer
This project is for informational purposes only and is not financial advice.
