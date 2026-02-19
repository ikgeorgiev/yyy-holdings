from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from compare import (
    compare_holdings,
    get_available_dates,
    get_available_funds,
    get_totals_for_date,
)

DB_PATH = "holdings.duckdb"
FUND_NAMES = {
    "YYY": "Amplify High Income ETF",
    "PCEF": "Invesco CEF Income Composite ETF",
}
FUND_SOURCES = {
    "YYY": "Amplify holdings feed.",
    "PCEF": "Invesco holdings API.",
}


def _format_currency(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.0f}"


def _format_delta(value: float, metric: str) -> str:
    if pd.isna(value):
        return "-"
    if metric == "Shares":
        return f"{value:+,.0f}"
    return f"{value:+,.2f}"


def _style_delta(value: float) -> str:
    if pd.isna(value):
        return ""
    if value > 0:
        return "color: #34d399; font-weight: 600;"
    if value < 0:
        return "color: #f87171; font-weight: 600;"
    return "color: rgba(244, 239, 231, 0.7);"


def _cash_mask(df: pd.DataFrame) -> pd.Series:
    ticker = df["ticker"].fillna("").astype(str)
    name = df["name"].fillna("").astype(str)
    return ticker.str.contains("cash", case=False) | name.str.contains(
        "cash", case=False
    )


def _ticker_mask(df: pd.DataFrame, ticker: str) -> pd.Series:
    tickers = df["ticker"].fillna("").astype(str)
    return tickers.str.upper().eq(ticker.upper())


def _position_mask(
    df: pd.DataFrame, hide_cash: bool, hide_agpxx: bool
) -> pd.Series:
    mask = pd.Series(False, index=df.index)
    if hide_cash:
        mask |= _cash_mask(df)
    if hide_agpxx:
        mask |= _ticker_mask(df, "AGPXX")
    return mask


def _totals_from_combined(df: pd.DataFrame, prefix: str) -> dict[str, float | int]:
    market_value_col = f"{prefix}_market_value"
    shares_col = f"{prefix}_shares"
    return {
        "total_aum": df[market_value_col].sum(skipna=True),
        "holdings_count": int(df[shares_col].notna().sum()),
    }


def _build_styler(
    df: pd.DataFrame, formats: dict[str, str], delta_columns: list[str] | None = None
) -> "pd.io.formats.style.Styler":
    styler = df.style.format(formats, na_rep="-")
    if delta_columns:
        styler = styler.applymap(_style_delta, subset=delta_columns)
    return styler.hide(axis="index")


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        @import url("https://fonts.googleapis.com/css2?family=Fraunces:wght@500;650;700&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap");

        :root {
            --ink: #f4efe7;
            --muted: rgba(244, 239, 231, 0.65);
            --accent: #2dd4bf;
            --accent-strong: #14b8a6;
            --accent-warm: #f59e0b;
            --panel: #14161a;
            --panel-border: rgba(255, 255, 255, 0.08);
        }

        .stApp {
            background:
                radial-gradient(1200px 520px at 2% -10%, rgba(45, 212, 191, 0.18) 0%, rgba(0,0,0,0) 60%),
                radial-gradient(900px 420px at 100% 0%, rgba(245, 158, 11, 0.16) 0%, rgba(0,0,0,0) 60%),
                linear-gradient(135deg, #0b0d10 0%, #101318 52%, #0f1116 100%);
            color: var(--ink);
            font-family: "IBM Plex Sans", sans-serif;
        }

        h1, h2, h3, h4 {
            font-family: "Fraunces", serif;
            letter-spacing: -0.01em;
            color: var(--ink);
        }

        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f1217 0%, #151922 100%);
            border-right: 1px solid var(--panel-border);
            color: var(--ink);
        }

        .hero-card {
            background: var(--panel);
            border: 1px solid var(--panel-border);
            border-radius: 18px;
            padding: 22px 24px;
            box-shadow: 0 18px 40px rgba(0, 0, 0, 0.45);
        }

        .eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.3em;
            font-size: 0.7rem;
            color: var(--muted);
        }

        .hero-title {
            font-size: 2.3rem;
            margin: 6px 0 6px 0;
        }

        .hero-sub {
            color: var(--muted);
            font-size: 1rem;
            margin-bottom: 12px;
        }

        .pill {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            background: rgba(45, 212, 191, 0.18);
            color: var(--accent);
            font-weight: 600;
            margin-right: 6px;
        }

        .metric-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 12px;
            margin-top: 16px;
        }

        .metric-block {
            background: var(--panel);
            border: 1px solid var(--panel-border);
            border-radius: 14px;
            padding: 12px 14px;
            box-shadow: 0 12px 24px rgba(0, 0, 0, 0.35);
        }

        .metric-label {
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.2em;
            color: var(--muted);
        }

        .metric-value {
            font-size: 1.35rem;
            font-weight: 700;
            margin-top: 6px;
        }

        .metric-delta {
            font-size: 0.85rem;
            margin-top: 4px;
            font-weight: 600;
        }

        .delta-pos { color: var(--accent); }
        .delta-neg { color: #f87171; }
        .delta-neutral { color: var(--muted); }

        div[data-testid="stPlotlyChart"],
        div[data-testid="stDataFrame"] {
            background: var(--panel);
            border: 1px solid var(--panel-border);
            border-radius: 16px;
            padding: 12px;
            box-shadow: 0 12px 30px rgba(0, 0, 0, 0.35);
        }

        div[data-testid="metric-container"] {
            background: var(--panel);
            border: 1px solid var(--panel-border);
            border-radius: 14px;
            padding: 12px 14px;
            box-shadow: 0 12px 24px rgba(0, 0, 0, 0.35);
        }

        div[role="tablist"] {
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid var(--panel-border);
            border-radius: 999px;
            padding: 6px 10px;
            gap: 6px;
        }

        div[role="tablist"] button {
            font-weight: 600;
            letter-spacing: 0.02em;
            color: var(--muted);
            background: transparent;
        }

        div[role="tablist"] button[aria-selected="true"] {
            color: var(--ink);
            box-shadow: inset 0 -2px 0 var(--accent);
        }

        .muted {
            color: var(--muted);
            font-size: 0.9rem;
        }

        #MainMenu { visibility: hidden; }
        footer { visibility: hidden; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(page_title="ETF Holdings Tracker", layout="wide")
    _inject_styles()

    if not Path(DB_PATH).exists():
        st.error("Database not found. Run ingest.py to create holdings.duckdb.")
        st.stop()

    funds = get_available_funds(DB_PATH)
    if not funds:
        st.warning("No holdings found. Run ingest.py to load the first snapshot.")
        st.stop()

    default_fund = "YYY" if "YYY" in funds else funds[0]

    st.sidebar.header("Compare snapshots")
    selected_fund = st.sidebar.selectbox(
        "Fund",
        funds,
        index=funds.index(default_fund),
    )

    dates = get_available_dates(selected_fund, DB_PATH)
    if not dates:
        st.warning(
            f"No holdings found for {selected_fund}. Run ingest.py to load snapshots."
        )
        st.stop()

    baseline_index = max(len(dates) - 2, 0)
    comparison_index = len(dates) - 1

    baseline_date = st.sidebar.selectbox(
        "Baseline Date",
        dates,
        index=baseline_index,
        format_func=lambda d: d.isoformat(),
    )
    comparison_date = st.sidebar.selectbox(
        "Comparison Date",
        dates,
        index=comparison_index,
        format_func=lambda d: d.isoformat(),
    )
    mover_metric = st.sidebar.radio(
        "Mover Metric",
        ["Shares", "Market Value"],
        horizontal=True,
    )
    top_n = st.sidebar.slider("Top movers", min_value=5, max_value=20, value=12)
    hide_cash = st.sidebar.toggle("Hide cash positions", value=False)
    exclude_cash_from_totals = False
    if hide_cash:
        exclude_cash_from_totals = st.sidebar.toggle(
            "Exclude cash from totals", value=False
        )
    hide_agpxx = st.sidebar.toggle("Hide AGPXX positions", value=False)
    exclude_agpxx_from_totals = False
    if hide_agpxx:
        exclude_agpxx_from_totals = st.sidebar.toggle(
            "Exclude AGPXX from totals", value=False
        )

    st.sidebar.markdown("---")
    st.sidebar.caption(f"Source: {FUND_SOURCES.get(selected_fund, 'Holdings feed.')}")

    if baseline_date == comparison_date:
        st.warning("Baseline and comparison dates are the same.")

    added_raw, removed_raw, changed_raw, combined_raw = compare_holdings(
        baseline_date, comparison_date, selected_fund, DB_PATH
    )
    added = added_raw
    removed = removed_raw
    changed = changed_raw
    combined = combined_raw
    if hide_cash or hide_agpxx:
        # Optional display filtering for cash/AGPXX rows.
        added = added_raw[
            ~_position_mask(added_raw, hide_cash, hide_agpxx)
        ].copy()
        removed = removed_raw[
            ~_position_mask(removed_raw, hide_cash, hide_agpxx)
        ].copy()
        changed = changed_raw[
            ~_position_mask(changed_raw, hide_cash, hide_agpxx)
        ].copy()
        combined = combined_raw[
            ~_position_mask(combined_raw, hide_cash, hide_agpxx)
        ].copy()

    baseline_totals = get_totals_for_date(baseline_date, selected_fund, DB_PATH)
    comparison_totals = get_totals_for_date(comparison_date, selected_fund, DB_PATH)
    exclude_for_totals = exclude_cash_from_totals or exclude_agpxx_from_totals
    if exclude_for_totals:
        combined_for_totals = combined_raw[
            ~_position_mask(
                combined_raw,
                exclude_cash_from_totals,
                exclude_agpxx_from_totals,
            )
        ].copy()
        baseline_totals = _totals_from_combined(combined_for_totals, "start")
        comparison_totals = _totals_from_combined(combined_for_totals, "end")

    added_for_counts = added
    removed_for_counts = removed
    if exclude_for_totals:
        added_for_counts = added_raw[
            ~_position_mask(
                added_raw,
                exclude_cash_from_totals,
                exclude_agpxx_from_totals,
            )
        ].copy()
        removed_for_counts = removed_raw[
            ~_position_mask(
                removed_raw,
                exclude_cash_from_totals,
                exclude_agpxx_from_totals,
            )
        ].copy()
    else:
        added_for_counts = added
        removed_for_counts = removed

    aum_delta = comparison_totals["total_aum"] - baseline_totals["total_aum"]
    holdings_delta = (
        comparison_totals["holdings_count"] - baseline_totals["holdings_count"]
    )

    aum_delta_class = (
        "delta-pos"
        if aum_delta > 0
        else "delta-neg" if aum_delta < 0 else "delta-neutral"
    )
    holdings_delta_class = (
        "delta-pos"
        if holdings_delta > 0
        else "delta-neg" if holdings_delta < 0 else "delta-neutral"
    )

    left_col, right_col = st.columns([2.2, 1.4], gap="large")

    fund_name = FUND_NAMES.get(selected_fund, selected_fund)

    with left_col:
        hero_html = f"""
        <div class="hero-card">
            <div class="eyebrow">{selected_fund} Holdings Report</div>
            <div class="hero-title">{fund_name}</div>
            <div class="hero-sub">Snapshot comparison and daily change analytics.</div>
            <div class="pill">Baseline: {baseline_date.isoformat()}</div>
            <div class="pill">Comparison: {comparison_date.isoformat()}</div>
            <div class="muted" style="margin-top:10px;">Snapshots in database: {len(dates)}</div>
        </div>
        """
        st.markdown(hero_html, unsafe_allow_html=True)

    with right_col:
        pulse_html = f"""
        <div class="metric-grid">
            <div class="metric-block">
                <div class="metric-label">Baseline AUM</div>
                <div class="metric-value">{_format_currency(baseline_totals["total_aum"])}</div>
                <div class="metric-delta delta-neutral">Starting point</div>
            </div>
            <div class="metric-block">
                <div class="metric-label">Comparison AUM</div>
                <div class="metric-value">{_format_currency(comparison_totals["total_aum"])}</div>
                <div class="metric-delta delta-neutral">Ending point</div>
            </div>
            <div class="metric-block">
                <div class="metric-label">AUM Change</div>
                <div class="metric-value">{_format_currency(aum_delta)}</div>
                <div class="metric-delta {aum_delta_class}">
                    {"Up" if aum_delta > 0 else "Down" if aum_delta < 0 else "Flat"}
                </div>
            </div>
            <div class="metric-block">
                <div class="metric-label">Holdings Delta</div>
                <div class="metric-value">{holdings_delta:+d}</div>
                <div class="metric-delta {holdings_delta_class}">
                    {len(added_for_counts)} new / {len(removed_for_counts)} sold
                </div>
            </div>
        </div>
        """
        st.markdown(pulse_html, unsafe_allow_html=True)

    st.subheader("Biggest Movers")
    movers = combined.copy()
    metric_column = "shares_delta" if mover_metric == "Shares" else "market_value_delta"
    movers = movers[movers[metric_column] != 0].copy()

    if movers.empty:
        st.info("No changes between the selected dates.")
    else:
        movers["abs_delta"] = movers[metric_column].abs()
        movers = movers.sort_values("abs_delta", ascending=False).head(top_n)
        movers = movers.sort_values(metric_column)
        colors = ["#f87171" if val < 0 else "#34d399" for val in movers[metric_column]]
        text_values = [
            _format_delta(val, mover_metric) for val in movers[metric_column]
        ]
        fig = go.Figure(
            go.Bar(
                y=movers["ticker"],
                x=movers[metric_column],
                orientation="h",
                marker_color=colors,
                text=text_values,
                textposition="outside",
                hovertemplate=(
                    "%{y}<br>%{x:,.0f}<extra></extra>"
                    if mover_metric == "Shares"
                    else "%{y}<br>$%{x:,.2f}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            yaxis_title=(
                "Share Change" if mover_metric == "Shares" else "Market Value Change"
            ),
            xaxis_title="Ticker",
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Sans", color="#f4efe7"),
            xaxis=dict(
                showgrid=True, gridcolor="rgba(255,255,255,0.08)", zeroline=True
            ),
            yaxis=dict(gridcolor="rgba(255,255,255,0.08)"),
            margin=dict(l=40, r=40, t=20, b=40),
            template="plotly_dark",
            height=520,
        )
        st.plotly_chart(fig, use_container_width=True)

    tab_all, tab_new, tab_sold, tab_changed = st.tabs(["All", "New", "Sold", "Changed"])

    with tab_all:
        if combined.empty:
            st.info("No holdings found for the selected dates.")
        else:
            display = combined[
                [
                    "ticker",
                    "name",
                    "status",
                    "start_shares",
                    "end_shares",
                    "shares_delta",
                    "start_market_value",
                    "end_market_value",
                    "market_value_delta",
                    "start_weight",
                    "end_weight",
                ]
            ].rename(
                columns={
                    "start_shares": "shares_start",
                    "end_shares": "shares_end",
                    "start_market_value": "market_value_start",
                    "end_market_value": "market_value_end",
                    "start_weight": "weight_start",
                    "end_weight": "weight_end",
                }
            )
            display = display.sort_values("market_value_delta", ascending=False)
            formats = {
                "shares_start": "{:,.0f}",
                "shares_end": "{:,.0f}",
                "shares_delta": "{:+,.0f}",
                "market_value_start": "${:,.2f}",
                "market_value_end": "${:,.2f}",
                "market_value_delta": "${:+,.2f}",
                "weight_start": "{:.2f}%",
                "weight_end": "{:.2f}%",
            }
            st.dataframe(
                _build_styler(
                    display,
                    formats,
                    delta_columns=["shares_delta", "market_value_delta"],
                ),
                use_container_width=True,
            )

    with tab_new:
        if added.empty:
            st.info("No new positions.")
        else:
            display = added[
                ["ticker", "name", "end_shares", "end_market_value", "end_weight"]
            ].rename(
                columns={
                    "end_shares": "shares",
                    "end_market_value": "market_value",
                    "end_weight": "weight",
                }
            )
            formats = {
                "shares": "{:,.0f}",
                "market_value": "${:,.2f}",
                "weight": "{:.2f}%",
            }
            st.dataframe(_build_styler(display, formats), use_container_width=True)

    with tab_sold:
        if removed.empty:
            st.info("No sold positions.")
        else:
            display = removed[
                ["ticker", "name", "start_shares", "start_market_value", "start_weight"]
            ].rename(
                columns={
                    "start_shares": "shares",
                    "start_market_value": "market_value",
                    "start_weight": "weight",
                }
            )
            formats = {
                "shares": "{:,.0f}",
                "market_value": "${:,.2f}",
                "weight": "{:.2f}%",
            }
            st.dataframe(_build_styler(display, formats), use_container_width=True)

    with tab_changed:
        if changed.empty:
            st.info("No changed positions.")
        else:
            display = changed[
                [
                    "ticker",
                    "name",
                    "start_shares",
                    "end_shares",
                    "shares_delta",
                    "start_market_value",
                    "end_market_value",
                    "market_value_delta",
                ]
            ].rename(
                columns={
                    "start_shares": "shares_start",
                    "end_shares": "shares_end",
                    "shares_delta": "shares_delta",
                    "start_market_value": "market_value_start",
                    "end_market_value": "market_value_end",
                    "market_value_delta": "market_value_delta",
                }
            )
            formats = {
                "shares_start": "{:,.0f}",
                "shares_end": "{:,.0f}",
                "shares_delta": "{:+,.0f}",
                "market_value_start": "${:,.2f}",
                "market_value_end": "${:,.2f}",
                "market_value_delta": "${:+,.2f}",
            }
            st.dataframe(
                _build_styler(
                    display,
                    formats,
                    delta_columns=["shares_delta", "market_value_delta"],
                ),
                use_container_width=True,
            )


if __name__ == "__main__":
    main()
