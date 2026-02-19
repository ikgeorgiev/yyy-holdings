from __future__ import annotations

from datetime import date
from typing import List, Tuple

import duckdb
import pandas as pd

DB_PATH = "holdings.duckdb"
DEFAULT_FUND_TICKER = "YYY"


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    try:
        return {
            row[1].lower()
            for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        }
    except duckdb.Error:
        return set()


def _empty_comparison_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ticker",
            "name",
            "start_shares",
            "end_shares",
            "start_market_value",
            "end_market_value",
            "start_weight",
            "end_weight",
            "status",
            "shares_delta",
            "market_value_delta",
        ]
    )


def get_available_funds(db_path: str = DB_PATH) -> List[str]:
    con = duckdb.connect(db_path, read_only=True)
    try:
        columns = _table_columns(con, "holdings")
        if not columns:
            return []
        if "fund" in columns:
            funds = con.execute(
                """
                SELECT DISTINCT fund
                FROM holdings
                WHERE fund IS NOT NULL AND fund <> ''
                ORDER BY fund
                """
            ).fetchall()
            return [row[0] for row in funds]
        row_count = con.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
        return [DEFAULT_FUND_TICKER] if row_count else []
    finally:
        con.close()


def get_available_dates(
    fund_ticker: str = DEFAULT_FUND_TICKER, db_path: str = DB_PATH
) -> List[date]:
    fund_ticker = fund_ticker.upper()
    con = duckdb.connect(db_path, read_only=True)
    try:
        columns = _table_columns(con, "holdings")
        if not columns:
            return []
        if "fund" in columns:
            dates = con.execute(
                """
                SELECT DISTINCT date
                FROM holdings
                WHERE UPPER(fund) = ?
                ORDER BY date
                """,
                [fund_ticker],
            ).fetchall()
        else:
            if fund_ticker != DEFAULT_FUND_TICKER:
                return []
            dates = con.execute(
                "SELECT DISTINCT date FROM holdings ORDER BY date"
            ).fetchall()
        return [row[0] for row in dates]
    finally:
        con.close()


def get_totals_for_date(
    target_date: date,
    fund_ticker: str = DEFAULT_FUND_TICKER,
    db_path: str = DB_PATH,
) -> dict:
    fund_ticker = fund_ticker.upper()
    con = duckdb.connect(db_path, read_only=True)
    try:
        columns = _table_columns(con, "holdings")
        if not columns:
            return {"total_aum": 0.0, "holdings_count": 0}
        if "fund" in columns:
            result = con.execute(
                """
                SELECT
                    COALESCE(SUM(market_value), 0) AS total_aum,
                    COUNT(*) AS holdings_count
                FROM holdings
                WHERE date = ? AND UPPER(fund) = ?
                """,
                [target_date, fund_ticker],
            ).fetchone()
        else:
            if fund_ticker != DEFAULT_FUND_TICKER:
                return {"total_aum": 0.0, "holdings_count": 0}
            result = con.execute(
                """
                SELECT
                    COALESCE(SUM(market_value), 0) AS total_aum,
                    COUNT(*) AS holdings_count
                FROM holdings
                WHERE date = ?
                """,
                [target_date],
            ).fetchone()
        return {"total_aum": result[0], "holdings_count": result[1]}
    finally:
        con.close()


def compare_holdings(
    start_date: date,
    end_date: date,
    fund_ticker: str = DEFAULT_FUND_TICKER,
    db_path: str = DB_PATH,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fund_ticker = fund_ticker.upper()
    con = duckdb.connect(db_path, read_only=True)
    try:
        columns = _table_columns(con, "holdings")
        if not columns:
            empty = _empty_comparison_frame()
            return empty.copy(), empty.copy(), empty.copy(), empty

        if "fund" in columns:
            query = """
                WITH
                start_snapshot AS (
                    SELECT * FROM holdings WHERE date = ? AND UPPER(fund) = ?
                ),
                end_snapshot AS (
                    SELECT * FROM holdings WHERE date = ? AND UPPER(fund) = ?
                ),
                joined AS (
                    SELECT
                        COALESCE(e.ticker, s.ticker) AS ticker,
                        COALESCE(e.name, s.name) AS name,
                        s.shares AS start_shares,
                        e.shares AS end_shares,
                        s.market_value AS start_market_value,
                        e.market_value AS end_market_value,
                        s.weight AS start_weight,
                        e.weight AS end_weight
                    FROM start_snapshot s
                    FULL OUTER JOIN end_snapshot e ON s.ticker = e.ticker
                )
                SELECT
                    *,
                    CASE
                        WHEN start_shares IS NULL THEN 'added'
                        WHEN end_shares IS NULL THEN 'removed'
                        ELSE 'changed'
                    END AS status,
                    COALESCE(end_shares, 0) - COALESCE(start_shares, 0) AS shares_delta,
                    COALESCE(end_market_value, 0) - COALESCE(start_market_value, 0) AS market_value_delta
                FROM joined
            """
            df = con.execute(
                query, [start_date, fund_ticker, end_date, fund_ticker]
            ).df()
        else:
            if fund_ticker != DEFAULT_FUND_TICKER:
                empty = _empty_comparison_frame()
                return empty.copy(), empty.copy(), empty.copy(), empty
            query = """
                WITH
                start_snapshot AS (
                    SELECT * FROM holdings WHERE date = ?
                ),
                end_snapshot AS (
                    SELECT * FROM holdings WHERE date = ?
                ),
                joined AS (
                    SELECT
                        COALESCE(e.ticker, s.ticker) AS ticker,
                        COALESCE(e.name, s.name) AS name,
                        s.shares AS start_shares,
                        e.shares AS end_shares,
                        s.market_value AS start_market_value,
                        e.market_value AS end_market_value,
                        s.weight AS start_weight,
                        e.weight AS end_weight
                    FROM start_snapshot s
                    FULL OUTER JOIN end_snapshot e ON s.ticker = e.ticker
                )
                SELECT
                    *,
                    CASE
                        WHEN start_shares IS NULL THEN 'added'
                        WHEN end_shares IS NULL THEN 'removed'
                        ELSE 'changed'
                    END AS status,
                    COALESCE(end_shares, 0) - COALESCE(start_shares, 0) AS shares_delta,
                    COALESCE(end_market_value, 0) - COALESCE(start_market_value, 0) AS market_value_delta
                FROM joined
            """
            df = con.execute(query, [start_date, end_date]).df()
    finally:
        con.close()

    added = df[df["status"] == "added"].copy()
    removed = df[df["status"] == "removed"].copy()
    changed = df[(df["status"] == "changed") & (df["shares_delta"] != 0)].copy()

    added = added.sort_values("market_value_delta", ascending=False)
    removed = removed.sort_values("market_value_delta")
    changed = changed.sort_values("market_value_delta", ascending=False)

    return added, removed, changed, df
