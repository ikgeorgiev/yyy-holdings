import argparse
from datetime import date, datetime
import html
from io import BytesIO, StringIO
import re
import time
from typing import Iterable, Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import duckdb
import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

DEFAULT_FUND_TICKER = "YYY"
DB_PATH = "holdings.duckdb"
FUND_CONFIGS: dict[str, dict[str, Optional[str]]] = {
    "YYY": {
        "name": "Amplify High Income ETF",
        "holdings_url": "https://amplifyetfs.com/yyy-holdings/",
        "feed_url": "https://amplifyetfs.com/wp-content/uploads/feeds/AmplifyWeb.40XL.XL_Holdings.csv",
        "download_csv_url": None,
        "source_label": "Amplify holdings feed",
    },
    "PCEF": {
        "name": "Invesco CEF Income Composite ETF",
        "holdings_url": "https://www.invesco.com/us/en/financial-products/etfs/invesco-cef-income-composite-etf.html#Portfolio",
        "api_url": "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/46138E404/holdings/fund?idType=cusip&productType=ETF",
        "profile_url": None,
        "feed_url": None,
        "download_csv_url": None,
        "source_label": "Invesco holdings API",
    },
}
HOLDINGS_URL = FUND_CONFIGS[DEFAULT_FUND_TICKER]["holdings_url"] or ""
HOLDINGS_FEED_URL = FUND_CONFIGS[DEFAULT_FUND_TICKER]["feed_url"] or ""


def get_supported_funds() -> list[str]:
    return sorted(FUND_CONFIGS)


def get_fund_config(fund_ticker: str) -> dict[str, Optional[str]]:
    ticker = fund_ticker.strip().upper()
    if ticker not in FUND_CONFIGS:
        supported = ", ".join(get_supported_funds())
        raise ValueError(
            f"Unsupported fund ticker '{fund_ticker}'. Supported values: {supported}"
        )
    return FUND_CONFIGS[ticker]


class HoldingRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    date: date
    fund: str = Field(min_length=1)
    ticker: str = Field(min_length=1)
    name: str = Field(min_length=1)
    shares: float
    market_value: float
    weight: float

    @field_validator("fund", "ticker", "name")
    @classmethod
    def _strip_strings(cls, value: str) -> str:
        return value.strip()

    @field_validator("fund", "ticker")
    @classmethod
    def _normalize_ticker(cls, value: str) -> str:
        return value.upper()


def _normalize_column(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def _find_csv_link(html: str, base_url: str) -> Optional[str]:
    match = re.search(r'href=["\']([^"\']+\.csv[^"\']*)["\']', html, re.IGNORECASE)
    if not match:
        return None
    return urljoin(base_url, match.group(1))


def _extract_fund_ticker(html: str) -> Optional[str]:
    match = re.search(r"AmplifyFundName\\s*=\\s*['\"]([^'\"]+)['\"]", html)
    if not match:
        return None
    return match.group(1).strip()


def _pick_holdings_table(tables: Iterable[pd.DataFrame]) -> pd.DataFrame:
    tables = list(tables)
    for table in tables:
        columns = [_normalize_column(col) for col in table.columns]
        if (
            "ticker" in columns
            or "symbol" in columns
            or "stockticker" in columns
            or "holdingticker" in columns
        ):
            return table
    return tables[0]


def _parse_number(value) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    text = text.replace(",", "")
    text = text.replace("$", "")
    text = text.replace("%", "")
    text = text.replace("(", "-").replace(")", "")
    multiplier = 1.0
    if text[-1:].lower() in {"k", "m", "b", "t"}:
        suffix = text[-1].lower()
        text = text[:-1]
        multiplier = {
            "k": 1_000.0,
            "m": 1_000_000.0,
            "b": 1_000_000_000.0,
            "t": 1_000_000_000_000.0,
        }[suffix]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def _coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(part) for part in col if str(part) != "nan"]).strip()
            for col in df.columns
        ]

    normalized = {_normalize_column(col): col for col in df.columns}

    mapping = {}
    if "ticker" in normalized:
        mapping[normalized["ticker"]] = "ticker"
    elif "symbol" in normalized:
        mapping[normalized["symbol"]] = "ticker"
    elif "holdingticker" in normalized:
        mapping[normalized["holdingticker"]] = "ticker"
    elif "stockticker" in normalized:
        mapping[normalized["stockticker"]] = "ticker"

    if "name" in normalized:
        mapping[normalized["name"]] = "name"
    elif "issuername" in normalized:
        mapping[normalized["issuername"]] = "name"
    elif "holding" in normalized:
        mapping[normalized["holding"]] = "name"
    elif "holdingname" in normalized:
        mapping[normalized["holdingname"]] = "name"
    elif "security" in normalized:
        mapping[normalized["security"]] = "name"
    elif "securityname" in normalized:
        mapping[normalized["securityname"]] = "name"
    if "securitytypename" in normalized:
        mapping[normalized["securitytypename"]] = "__name_fallback"

    if "shares" in normalized:
        mapping[normalized["shares"]] = "shares"
    elif "units" in normalized:
        mapping[normalized["units"]] = "shares"
    elif "shs" in normalized:
        mapping[normalized["shs"]] = "shares"
    elif "sharesparvalue" in normalized:
        mapping[normalized["sharesparvalue"]] = "shares"
    if "cusip" in normalized:
        mapping[normalized["cusip"]] = "__ticker_fallback"

    if "marketvalue" in normalized:
        mapping[normalized["marketvalue"]] = "market_value"
    elif "marketvaluebase" in normalized:
        mapping[normalized["marketvaluebase"]] = "market_value"
    elif "marketvalueusd" in normalized:
        mapping[normalized["marketvalueusd"]] = "market_value"

    if "weight" in normalized:
        mapping[normalized["weight"]] = "weight"
    elif "weighting" in normalized:
        mapping[normalized["weighting"]] = "weight"
    elif "weightings" in normalized:
        mapping[normalized["weightings"]] = "weight"
    elif "percentofnav" in normalized:
        mapping[normalized["percentofnav"]] = "weight"
    elif "weightofnav" in normalized:
        mapping[normalized["weightofnav"]] = "weight"
    elif "percentofnetassets" in normalized:
        mapping[normalized["percentofnetassets"]] = "weight"
    elif "percentageoftotalnetassets" in normalized:
        mapping[normalized["percentageoftotalnetassets"]] = "weight"
    elif "pctofnav" in normalized:
        mapping[normalized["pctofnav"]] = "weight"
    elif "percentmarketvalue" in normalized:
        mapping[normalized["percentmarketvalue"]] = "weight"
    else:
        weight_like = [
            key
            for key in normalized
            if "weight" in key and "average" not in key and "avg" not in key
        ]
        if weight_like:
            mapping[normalized[weight_like[0]]] = "weight"

    df = df.rename(columns=mapping)
    required = ["ticker", "name", "shares", "market_value"]
    missing = set(required).difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if "weight" not in df.columns:
        df["weight"] = None

    if "__ticker_fallback" not in df.columns:
        df["__ticker_fallback"] = None
    if "__name_fallback" not in df.columns:
        df["__name_fallback"] = None

    invalid_markers = {"", "NAN", "NONE", "NULL", "-", "--", "N/A", "NA"}
    invalid_ticker_markers = invalid_markers | {"TOTAL"}

    ticker_text = df["ticker"].map(lambda value: "" if pd.isna(value) else str(value).strip())
    ticker_norm = ticker_text.str.upper()
    ticker_fallback = df["__ticker_fallback"].map(
        lambda value: "" if pd.isna(value) else str(value).strip().upper()
    )
    invalid_ticker_mask = ticker_norm.isin(invalid_ticker_markers)
    valid_ticker_fallback = ~ticker_fallback.isin(invalid_ticker_markers)
    ticker_norm = ticker_norm.where(
        ~invalid_ticker_mask,
        ticker_fallback.where(valid_ticker_fallback, "UNINVESTED"),
    )
    df["ticker"] = ticker_norm

    name_text = df["name"].map(lambda value: "" if pd.isna(value) else str(value).strip())
    name_fallback = df["__name_fallback"].map(
        lambda value: "" if pd.isna(value) else str(value).strip()
    )
    invalid_name_mask = name_text.str.upper().isin(invalid_markers)
    valid_name_fallback = ~name_fallback.str.upper().isin(invalid_markers)
    name_text = name_text.where(
        ~invalid_name_mask,
        name_fallback.where(valid_name_fallback, "Unspecified Position"),
    )
    df["name"] = name_text

    # Invesco may return an internal cash placeholder row with null ticker/name.
    # Promote it to a stable, user-friendly label for dashboard display.
    uninvested_cash_mask = df["ticker"].astype(str).str.upper().eq("BNYMLEND") | (
        df["name"].astype(str).str.strip().str.upper().eq("UNINVESTIBLE CASH")
    )
    if uninvested_cash_mask.any():
        df.loc[uninvested_cash_mask, "ticker"] = "UNINVESTED_CASH"
        df.loc[uninvested_cash_mask, "name"] = "Uninvested Cash"

    df = df[required + ["weight"]].copy()
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["ticker"].notna() & (df["ticker"] != "")]
    df = df[df["ticker"].str.lower() != "nan"]
    df = df[df["ticker"].str.lower() != "total"]

    df["shares"] = df["shares"].map(_parse_number)
    df["market_value"] = df["market_value"].map(_parse_number)
    df["weight"] = df["weight"].map(_parse_number)

    if df["weight"].isna().any():
        total_market_value = df["market_value"].sum(skipna=True)
        if total_market_value:
            missing_mask = df["weight"].isna()
            df.loc[missing_mask, "weight"] = (
                df.loc[missing_mask, "market_value"] / total_market_value * 100
            )

    df = df.dropna(subset=["shares", "market_value", "weight"])

    return df


def _extract_as_of_date(df: pd.DataFrame) -> Optional[date]:
    if df.empty:
        return None
    normalized = {_normalize_column(col): col for col in df.columns}
    for key in ("date", "asofdate", "asof"):
        if key in normalized:
            series = df[normalized[key]].dropna()
            if series.empty:
                continue
            parsed = pd.to_datetime(series.iloc[0], errors="coerce")
            if pd.notna(parsed):
                return parsed.date()
    return None


def _fetch_holdings_feed(
    fund_ticker: str, headers: dict[str, str], feed_url: Optional[str]
) -> Optional[pd.DataFrame]:
    if not feed_url:
        return None

    try:
        response = requests.get(feed_url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        return None

    df = pd.read_csv(BytesIO(response.content))
    fund_ticker = fund_ticker.upper()
    if "Account" in df.columns:
        df = df[df["Account"].astype(str).str.upper() == fund_ticker]
    elif "Account Ticker" in df.columns:
        df = df[df["Account Ticker"].astype(str).str.upper() == fund_ticker]
    elif "Fund Ticker" in df.columns:
        df = df[df["Fund Ticker"].astype(str).str.upper() == fund_ticker]
    return df


def _fetch_csv(csv_url: Optional[str], headers: dict[str, str]) -> Optional[pd.DataFrame]:
    if not csv_url:
        return None
    try:
        response = requests.get(csv_url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        return None
    try:
        return pd.read_csv(BytesIO(response.content))
    except Exception:
        return None


def _fetch_invesco_holdings_api(
    api_url: Optional[str], headers: dict[str, str]
) -> Optional[pd.DataFrame]:
    if not api_url:
        return None

    def _with_cache_buster(url: str, salt: int) -> str:
        split_url = urlsplit(url)
        query = dict(parse_qsl(split_url.query, keep_blank_values=True))
        query["cb"] = str(int(time.time() * 1000) + salt)
        return urlunsplit(
            (
                split_url.scheme,
                split_url.netloc,
                split_url.path,
                urlencode(query),
                split_url.fragment,
            )
        )

    payload = None
    request_header_variants: list[dict[str, str]] = [
        {},  # Invesco often rejects browser-like headers with 406.
        {"User-Agent": headers.get("User-Agent", "python-requests")},
    ]
    for attempt in range(4):
        request_url = _with_cache_buster(api_url, attempt)
        for request_headers in request_header_variants:
            try:
                response = requests.get(request_url, headers=request_headers, timeout=30)
                response.raise_for_status()
                payload = response.json()
                break
            except (requests.RequestException, ValueError):
                continue
        if payload is not None:
            break
        time.sleep(0.8)

    if payload is None:
        return None

    holdings = payload.get("holdings")
    if not isinstance(holdings, list) or not holdings:
        return None

    df = pd.DataFrame(holdings)
    if df.empty:
        return None

    if "issuerName" in df.columns:
        df["issuerName"] = df["issuerName"].map(
            lambda value: html.unescape(str(value)) if value is not None else value
        )
    if payload.get("effectiveDate"):
        df["as_of_date"] = payload["effectiveDate"]
    return df


def _extract_assets_from_tables(tables: list[pd.DataFrame]) -> Optional[float]:
    for table in tables:
        if table.shape[1] < 2:
            continue
        first_col = table.columns[0]
        second_col = table.columns[1]
        labels = table[first_col].astype(str).str.strip().str.lower()
        asset_mask = labels.isin({"assets", "net assets", "aum", "total assets"})
        if asset_mask.any():
            raw_value = table.loc[asset_mask, second_col].iloc[0]
            parsed_value = _parse_number(raw_value)
            if parsed_value is not None:
                return parsed_value
    return None


def _fetch_pcef_holdings(
    holdings_url: str, profile_url: Optional[str], headers: dict[str, str]
) -> Optional[pd.DataFrame]:
    try:
        holdings_response = requests.get(holdings_url, headers=headers, timeout=30)
        holdings_response.raise_for_status()
    except requests.RequestException:
        return None

    try:
        tables = pd.read_html(StringIO(holdings_response.text))
    except ValueError:
        return None
    if not tables:
        return None

    base = _pick_holdings_table(tables).copy()
    if base.empty:
        return None

    total_assets = None
    if profile_url:
        try:
            profile_response = requests.get(profile_url, headers=headers, timeout=30)
            profile_response.raise_for_status()
            profile_tables = pd.read_html(StringIO(profile_response.text))
            total_assets = _extract_assets_from_tables(profile_tables)
        except (requests.RequestException, ValueError):
            total_assets = None

    normalized = {_normalize_column(col): col for col in base.columns}
    weight_column = normalized.get("weight")
    if not weight_column:
        return None

    weight_values = base[weight_column].map(_parse_number)
    weight_sum = float(weight_values.sum(skipna=True))
    if total_assets is not None and total_assets > 0:
        base["Market Value"] = weight_values / 100.0 * total_assets

        # StockAnalysis exposes only a subset of holdings without a paid plan.
        # Add a synthetic bucket so totals still reconcile to total assets.
        if 0 < weight_sum < 95:
            ticker_column = (
                normalized.get("ticker")
                or normalized.get("symbol")
                or normalized.get("holdingticker")
            )
            name_column = (
                normalized.get("name")
                or normalized.get("holding")
                or normalized.get("holdingname")
            )
            shares_column = normalized.get("shares")
            missing_weight = max(0.0, 100.0 - weight_sum)
            missing_value = total_assets * missing_weight / 100.0
            synthetic = {}
            if ticker_column:
                synthetic[ticker_column] = "OTHER"
            if name_column:
                synthetic[name_column] = "Other holdings (source-limited)"
            if shares_column:
                synthetic[shares_column] = 0.0
            synthetic[weight_column] = missing_weight
            synthetic["Market Value"] = missing_value
            base = pd.concat([base, pd.DataFrame([synthetic])], ignore_index=True)
    else:
        # Fallback keeps ingest usable if total assets is temporarily unavailable.
        base["Market Value"] = weight_values

    return base


def fetch_holdings(
    url: str,
    fund_ticker: str = DEFAULT_FUND_TICKER,
    feed_url: Optional[str] = HOLDINGS_FEED_URL,
    direct_csv_url: Optional[str] = None,
    profile_url: Optional[str] = None,
    api_url: Optional[str] = None,
) -> pd.DataFrame:
    fund_ticker = fund_ticker.upper()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    if fund_ticker == "PCEF":
        invesco_df = _fetch_invesco_holdings_api(api_url, headers)
        if invesco_df is not None and not invesco_df.empty:
            return invesco_df
        raise ValueError("Unable to fetch full PCEF holdings from Invesco API.")

    direct_csv = _fetch_csv(direct_csv_url, headers)
    if direct_csv is not None and not direct_csv.empty:
        return direct_csv

    html = None
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").lower()
        if "text/csv" in content_type or url.lower().endswith(".csv"):
            return pd.read_csv(BytesIO(response.content))
        html = response.text
        fund_ticker = _extract_fund_ticker(html) or fund_ticker
    except requests.RequestException:
        html = None

    if html:
        csv_link = _find_csv_link(html, url)
        if csv_link:
            csv_df = _fetch_csv(csv_link, headers)
            if csv_df is not None and not csv_df.empty:
                return csv_df

    feed_df = _fetch_holdings_feed(fund_ticker, headers, feed_url)
    if feed_df is not None and not feed_df.empty:
        return feed_df
    if html:
        tables = pd.read_html(StringIO(html))
        if not tables:
            raise ValueError("No tables found on holdings page.")
        return _pick_holdings_table(tables)

    raise ValueError("No holdings data found.")


def validate_holdings(
    df: pd.DataFrame,
    as_of_date: Optional[date],
    fund_ticker: str = DEFAULT_FUND_TICKER,
) -> pd.DataFrame:
    if as_of_date is None:
        as_of_date = _extract_as_of_date(df) or date.today()

    fund_ticker = fund_ticker.strip().upper()
    df = _coerce_columns(df)
    df["date"] = as_of_date
    df["fund"] = fund_ticker

    adapter = TypeAdapter(list[HoldingRecord])
    records = adapter.validate_python(df.to_dict(orient="records"))
    validated = pd.DataFrame([record.model_dump() for record in records])
    return validated.drop_duplicates(subset=["date", "fund", "ticker"], keep="last")


def upsert_holdings(df: pd.DataFrame, db_path: str) -> None:
    if df.empty:
        raise ValueError("No holdings rows to load.")
    if "fund" not in df.columns:
        raise ValueError("Missing required column: fund")

    holding_date = df["date"].iloc[0]
    holding_fund = str(df["fund"].iloc[0]).upper()
    con = duckdb.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS holdings (
            date DATE,
            fund VARCHAR,
            ticker VARCHAR,
            name VARCHAR,
            shares DOUBLE,
            market_value DOUBLE,
            weight DOUBLE
        )
        """
    )

    columns = {
        row[1].lower() for row in con.execute("PRAGMA table_info('holdings')").fetchall()
    }
    if "fund" not in columns:
        con.execute("ALTER TABLE holdings ADD COLUMN fund VARCHAR")
        con.execute(
            "UPDATE holdings SET fund = ? WHERE fund IS NULL",
            [DEFAULT_FUND_TICKER],
        )

    # Remove old source-limited PCEF snapshots that included a synthetic OTHER row.
    if holding_fund == "PCEF" and not (df["ticker"].astype(str).str.upper() == "OTHER").any():
        partial_dates = con.execute(
            """
            SELECT DISTINCT date
            FROM holdings
            WHERE UPPER(fund) = 'PCEF'
              AND UPPER(ticker) = 'OTHER'
            """
        ).fetchall()
        for (partial_date,) in partial_dates:
            con.execute(
                "DELETE FROM holdings WHERE date = ? AND UPPER(fund) = 'PCEF'",
                [partial_date],
            )

    con.execute("DELETE FROM holdings WHERE date = ? AND fund = ?", [holding_date, holding_fund])
    con.register("incoming_holdings", df)
    con.execute(
        """
        INSERT INTO holdings (date, fund, ticker, name, shares, market_value, weight)
        SELECT date, fund, ticker, name, shares, market_value, weight
        FROM incoming_holdings
        """
    )
    con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest ETF holdings into DuckDB.")
    parser.add_argument(
        "--fund",
        dest="fund",
        default=DEFAULT_FUND_TICKER,
        help=f"Fund ticker to ingest ({', '.join(get_supported_funds())}).",
    )
    parser.add_argument(
        "--all-funds",
        action="store_true",
        help="Ingest all configured funds.",
    )
    parser.add_argument("--date", dest="as_of_date", help="Override as-of date (YYYY-MM-DD).")
    parser.add_argument("--db", dest="db_path", default=DB_PATH, help="DuckDB file path.")
    parser.add_argument(
        "--url",
        dest="url",
        help="Override holdings page URL for the selected fund.",
    )
    args = parser.parse_args()

    if args.all_funds and args.url:
        parser.error("--url cannot be used with --all-funds.")

    if args.as_of_date:
        as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    else:
        as_of_date = None

    funds = get_supported_funds() if args.all_funds else [args.fund.strip().upper()]

    for fund in funds:
        fund_config = get_fund_config(fund)
        holdings_url = args.url or fund_config["holdings_url"] or HOLDINGS_URL
        feed_url = fund_config["feed_url"]
        direct_csv_url = fund_config["download_csv_url"]
        profile_url = fund_config.get("profile_url")
        api_url = fund_config.get("api_url")

        raw = fetch_holdings(
            holdings_url,
            fund_ticker=fund,
            feed_url=feed_url,
            direct_csv_url=direct_csv_url,
            profile_url=profile_url,
            api_url=api_url,
        )
        validated = validate_holdings(raw, as_of_date, fund_ticker=fund)
        upsert_holdings(validated, args.db_path)
        holding_date = validated["date"].iloc[0]
        print(
            f"Ingested {len(validated)} holdings for {fund} on {holding_date} into {args.db_path}."
        )


if __name__ == "__main__":
    main()
