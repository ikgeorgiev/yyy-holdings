import argparse
from datetime import date, datetime
from pathlib import Path
import re
from typing import Iterable, Optional

import pandas as pd

from ingest import (
    DEFAULT_FUND_TICKER,
    DB_PATH,
    _extract_as_of_date,
    _pick_holdings_table,
    upsert_holdings,
    validate_holdings,
)


def _infer_date_from_filename(path: Path) -> Optional[date]:
    name = path.stem
    patterns = [
        r"(?P<year>\d{4})[-_](?P<month>\d{2})[-_](?P<day>\d{2})",
        r"(?P<month>\d{2})[-_](?P<day>\d{2})[-_](?P<year>\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, name)
        if not match:
            continue
        parts = match.groupdict()
        try:
            return date(int(parts["year"]), int(parts["month"]), int(parts["day"]))
        except ValueError:
            continue
    return None


def _detect_csv_header_row(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        for idx, line in enumerate(handle):
            cleaned = line.strip().lower().replace('"', "").replace("'", "")
            if not cleaned:
                continue
            if "ticker" in cleaned and "name" in cleaned and "market" in cleaned:
                return idx
    return 0


def _load_holdings_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        header_row = _detect_csv_header_row(path)
        return pd.read_csv(
            path,
            skiprows=header_row,
            header=0,
            engine="python",
            encoding="utf-8-sig",
        )

    workbook = pd.ExcelFile(path)
    tables: list[pd.DataFrame] = []
    for sheet in workbook.sheet_names:
        df = workbook.parse(sheet)
        if not df.empty:
            tables.append(df)
    if not tables:
        return pd.DataFrame()
    return _pick_holdings_table(tables)


def _iter_input_files(path: Path, recursive: bool) -> Iterable[Path]:
    exts = {".xlsx", ".xlsm", ".xls", ".csv"}
    if path.is_file():
        return [path]
    if recursive:
        return sorted(p for p in path.rglob("*") if p.suffix.lower() in exts)
    return sorted(p for p in path.iterdir() if p.suffix.lower() in exts)


def ingest_holdings_file(
    path: Path, db_path: str, date_override: Optional[date], fund_ticker: str
) -> pd.DataFrame:
    df = _load_holdings_table(path)
    if df.empty:
        raise ValueError("No rows found in input file.")

    as_of_date = date_override or _infer_date_from_filename(path)
    if as_of_date is None:
        as_of_date = _extract_as_of_date(df)
    if as_of_date is None:
        raise ValueError("Unable to infer as-of date.")

    validated = validate_holdings(df, as_of_date, fund_ticker=fund_ticker)
    upsert_holdings(validated, db_path)
    return validated


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill ETF holdings from Excel/CSV files."
    )
    parser.add_argument(
        "path", help="Excel/CSV file or directory containing Excel/CSV files."
    )
    parser.add_argument("--db", dest="db_path", default=DB_PATH)
    parser.add_argument(
        "--fund",
        dest="fund",
        default=DEFAULT_FUND_TICKER,
        help="Fund ticker to tag rows with (e.g. YYY, PCEF).",
    )
    parser.add_argument(
        "--date",
        dest="as_of_date",
        help="Override as-of date for all files (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--recursive", action="store_true", help="Scan subdirectories for Excel files."
    )
    args = parser.parse_args()

    target_path = Path(args.path)
    if not target_path.exists():
        raise SystemExit(f"Path not found: {target_path}")

    date_override = None
    if args.as_of_date:
        date_override = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    fund_ticker = args.fund.strip().upper()

    files = list(_iter_input_files(target_path, args.recursive))
    if not files:
        raise SystemExit("No Excel/CSV files found.")

    failures = 0
    for file_path in files:
        try:
            validated = ingest_holdings_file(
                file_path, args.db_path, date_override, fund_ticker
            )
            holding_date = validated["date"].iloc[0]
            print(
                f"Ingested {len(validated)} holdings for {fund_ticker} on {holding_date} from {file_path.name}."
            )
        except Exception as exc:
            failures += 1
            print(f"Failed {file_path.name}: {exc}")

    if failures:
        raise SystemExit(f"{failures} file(s) failed to ingest.")


if __name__ == "__main__":
    main()
