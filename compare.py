from __future__ import annotations

from datetime import date
from typing import List, Tuple

import duckdb
import pandas as pd

DB_PATH = "holdings.duckdb"


def get_available_dates(db_path: str = DB_PATH) -> List[date]:
    con = duckdb.connect(db_path, read_only=True)
    dates = con.execute("SELECT DISTINCT date FROM holdings ORDER BY date").fetchall()
    con.close()
    return [row[0] for row in dates]


def get_totals_for_date(target_date: date, db_path: str = DB_PATH) -> dict:
    con = duckdb.connect(db_path, read_only=True)
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
    con.close()
    return {"total_aum": result[0], "holdings_count": result[1]}


def compare_holdings(
    start_date: date, end_date: date, db_path: str = DB_PATH
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    con = duckdb.connect(db_path, read_only=True)
    query = """
        WITH
        start AS (
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
            FROM start s
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
    con.close()

    added = df[df["status"] == "added"].copy()
    removed = df[df["status"] == "removed"].copy()
    changed = df[(df["status"] == "changed") & (df["shares_delta"] != 0)].copy()

    added = added.sort_values("market_value_delta", ascending=False)
    removed = removed.sort_values("market_value_delta")
    changed = changed.sort_values("market_value_delta", ascending=False)

    return added, removed, changed, df
