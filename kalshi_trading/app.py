"""
Streamlit dashboard for the Kalshi weather trading bot.

Pages (sidebar navigation):
  - Dashboard   : balance, open positions, recent orders
  - Signals     : live NOAA/Kalshi signals with one-click order placement
  - Backtest    : calibration, P&L, and theoretical P&L results / run buttons
  - Config      : editable trading parameters + NOAA forecast snapshot

Run locally:
    streamlit run app.py

Deploy to Railway — set these environment variables:
    KALSHI_API_KEY_ID
    KALSHI_PRIVATE_KEY   (full PEM text; Railway replaces \\n with real newlines at runtime)
    KALSHI_DEMO          (true / false)
"""

import io
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Page config — must be the first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Kalshi Weather Bot",
    page_icon="⛅",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Lazy imports so we surface import errors nicely
# ---------------------------------------------------------------------------
try:
    from client import KalshiClient
    from markets import get_open_weather_markets
    from strategy import get_signals, get_noaa_forecast, SERIES_TO_CITY
    from backtest import (
        run_calibration,
        run_pnl_backtest,
        run_theoretical_pnl,
        PRICE_HISTORY_FILE,
        CALIBRATION_FILE,
    )
    import config as _config_module
except Exception as _import_err:
    st.error(f"Import error: {_import_err}")
    st.stop()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PRICE_HISTORY_PATH  = Path(PRICE_HISTORY_FILE)
CALIBRATION_PATH    = Path(CALIBRATION_FILE)
THEORETICAL_PATH    = Path("theoretical_pnl.csv")


@st.cache_resource(show_spinner=False)
def get_client() -> KalshiClient:
    """Initialise (and cache) the Kalshi API client once per session."""
    return KalshiClient()


@st.cache_data(ttl=60, show_spinner=False)
def cached_balance() -> dict:
    return get_client().get_balance()


@st.cache_data(ttl=60, show_spinner=False)
def cached_positions() -> list:
    return get_client().get_positions()


@st.cache_data(ttl=60, show_spinner=False)
def cached_orders(status: str = None) -> list:
    return get_client().get_orders(status=status)


@st.cache_data(ttl=60, show_spinner=False)
def cached_markets() -> list:
    return get_open_weather_markets(get_client())


@st.cache_data(ttl=60, show_spinner=False)
def cached_signals(balance_dollars: float) -> list:
    markets = cached_markets()
    return get_signals(markets, balance_dollars)


@st.cache_data(ttl=300, show_spinner=False)
def cached_noaa_forecasts() -> dict:
    """Return {city: forecast_period_dict} for all tracked cities."""
    results = {}
    for city in set(SERIES_TO_CITY.values()):
        try:
            fc = get_noaa_forecast(city)
            results[city] = fc
        except Exception as exc:
            results[city] = {"error": str(exc)}
    return results


def _dollars(cents_or_none) -> str:
    """Format a cents integer (Kalshi balance) as a dollar string."""
    if cents_or_none is None:
        return "—"
    return f"${cents_or_none / 100:,.2f}"


def _pct(value) -> str:
    try:
        return f"{float(value):.1%}"
    except (TypeError, ValueError):
        return "—"


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

PAGES = ["Dashboard", "Signals", "Backtest", "Config"]
page = st.sidebar.radio("Navigate", PAGES)

st.sidebar.markdown("---")
demo_flag = os.getenv("KALSHI_DEMO", "true").lower() == "true"
st.sidebar.caption(f"Mode: **{'DEMO' if demo_flag else 'LIVE'}**")
st.sidebar.caption(f"Last render: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

# ---------------------------------------------------------------------------
# PAGE: Dashboard
# ---------------------------------------------------------------------------

if page == "Dashboard":
    st.title("Dashboard")

    # --- Balance ---
    st.subheader("Account Balance")
    try:
        bal = cached_balance()
        balance_cents = bal.get("balance", 0)
        col1, col2, col3 = st.columns(3)
        col1.metric("Balance", _dollars(balance_cents))
        col2.metric("Portfolio Value", _dollars(bal.get("portfolio_value")))
        col3.metric("Buying Power", _dollars(bal.get("available_balance", balance_cents)))
    except Exception as exc:
        st.error(f"Could not fetch balance: {exc}")

    st.divider()

    # --- Open Positions ---
    st.subheader("Open Positions")
    try:
        positions = cached_positions()
        if not positions:
            st.info("No open positions.")
        else:
            df_pos = pd.DataFrame(positions)
            # Surface the most useful columns first if they exist
            priority = ["ticker", "side", "quantity", "market_exposure", "realized_pnl", "unrealized_pnl"]
            ordered = [c for c in priority if c in df_pos.columns] + [
                c for c in df_pos.columns if c not in priority
            ]
            st.dataframe(df_pos[ordered], use_container_width=True)
    except Exception as exc:
        st.error(f"Could not fetch positions: {exc}")

    st.divider()

    # --- Recent Orders/Fills ---
    st.subheader("Recent Orders")
    try:
        orders = cached_orders()
        if not orders:
            st.info("No orders found.")
        else:
            df_ord = pd.DataFrame(orders)
            priority = [
                "order_id", "ticker", "action", "side", "type",
                "count", "yes_price", "no_price", "status", "created_time",
            ]
            ordered = [c for c in priority if c in df_ord.columns] + [
                c for c in df_ord.columns if c not in priority
            ]
            st.dataframe(df_ord[ordered].head(50), use_container_width=True)
    except Exception as exc:
        st.error(f"Could not fetch orders: {exc}")


# ---------------------------------------------------------------------------
# PAGE: Signals
# ---------------------------------------------------------------------------

elif page == "Signals":
    st.title("Trade Signals")

    if st.button("Refresh Signals"):
        cached_markets.clear()
        cached_signals.clear()
        cached_balance.clear()
        st.rerun()

    # Fetch balance for position sizing
    balance_dollars = 0.0
    try:
        bal = cached_balance()
        balance_dollars = bal.get("balance", 0) / 100
    except Exception as exc:
        st.warning(f"Could not fetch balance for position sizing: {exc}")

    # Fetch signals
    try:
        with st.spinner("Fetching markets and computing signals…"):
            signals = cached_signals(balance_dollars)
    except Exception as exc:
        st.error(f"Could not fetch signals: {exc}")
        signals = []

    if not signals:
        st.info("No signals at this time. The market may be closed or there is insufficient edge.")
    else:
        st.success(f"Found **{len(signals)}** signal(s).")

        display_cols = ["ticker", "side", "entry_price", "forecast_prob", "market_price", "edge", "contracts"]

        # Build a display dataframe
        df_sig = pd.DataFrame(
            [
                {
                    "ticker":        s["ticker"],
                    "side":          s["side"],
                    "entry_price":   s.get("price", s.get("market_price")),
                    "forecast_prob": s.get("forecast_prob"),
                    "market_price":  s.get("market_price"),
                    "edge":          round(s.get("edge", 0), 4),
                    "contracts":     s.get("contracts"),
                    "_price_cents":  s.get("price_cents"),
                }
                for s in signals
            ]
        )

        st.dataframe(
            df_sig[display_cols].style.format(
                {
                    "entry_price":   "{:.3f}",
                    "forecast_prob": "{:.1%}",
                    "market_price":  "{:.3f}",
                    "edge":          "{:.1%}",
                }
            ),
            use_container_width=True,
        )

        st.divider()
        st.subheader("Place an Order")

        # Order placement — one expander per signal
        for idx, sig in enumerate(signals):
            ticker    = sig["ticker"]
            side      = sig["side"]
            contracts = sig.get("contracts", 1)
            price_c   = sig.get("price_cents", round(sig.get("price", 0.5) * 100))

            with st.expander(f"{ticker}  |  BUY {side.upper()}  @  {sig.get('price', 0):.3f}  ({contracts} contracts)"):
                st.markdown(
                    f"**Ticker:** `{ticker}`  |  **Side:** `{side}`  |  "
                    f"**Forecast:** {_pct(sig.get('forecast_prob'))}  |  "
                    f"**Edge:** {_pct(sig.get('edge'))}"
                )

                col_c, col_p = st.columns(2)
                n_contracts = col_c.number_input(
                    "Contracts", min_value=1, max_value=100, value=int(contracts), key=f"cnt_{idx}"
                )
                limit_price = col_p.number_input(
                    "Limit price (cents)", min_value=1, max_value=99,
                    value=int(price_c), key=f"price_{idx}"
                )

                st.warning(
                    f"You are about to place a **BUY {side.upper()}** limit order on "
                    f"`{ticker}` for **{n_contracts} contract(s) @ {limit_price}¢**."
                )
                confirmed = st.checkbox("I confirm this order", key=f"confirm_{idx}")
                if st.button("Place Order", key=f"place_{idx}", disabled=not confirmed):
                    try:
                        order_kwargs = {
                            "ticker":      ticker,
                            "side":        side,
                            "count":       n_contracts,
                            "order_type":  "limit",
                        }
                        if side == "yes":
                            order_kwargs["yes_price"] = limit_price
                        else:
                            order_kwargs["no_price"] = limit_price

                        result = get_client().place_order(**order_kwargs)
                        st.success(f"Order placed! Response: `{result}`")
                        cached_orders.clear()
                        cached_positions.clear()
                    except Exception as exc:
                        st.error(f"Order failed: {exc}")


# ---------------------------------------------------------------------------
# PAGE: Backtest
# ---------------------------------------------------------------------------

elif page == "Backtest":
    st.title("Backtest")

    tab_cal, tab_pnl, tab_theo = st.tabs(
        ["Calibration", "P&L (collected prices)", "Theoretical P&L"]
    )

    # ------------------------------------------------------------------
    # TAB: Calibration
    # ------------------------------------------------------------------
    with tab_cal:
        st.subheader("Calibration Backtest")
        st.caption("Checks whether the model's signal direction matches settlement results using actual observed temperatures as a perfect forecast.")

        # Summary metrics if file exists
        if CALIBRATION_PATH.exists():
            try:
                df_cal = pd.read_csv(CALIBRATION_PATH)
                signalled = df_cal[df_cal["signal_side"].notna() & df_cal["signal_correct"].notna()]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total markets", len(df_cal))
                c2.metric("Signalled", len(signalled))
                c3.metric(
                    "Signal rate",
                    f"{len(signalled)/len(df_cal):.0%}" if len(df_cal) else "—",
                )
                c4.metric(
                    "Direction accuracy",
                    f"{signalled['signal_correct'].mean():.1%}" if len(signalled) else "—",
                )
                st.dataframe(df_cal, use_container_width=True)
            except Exception as exc:
                st.error(f"Could not load {CALIBRATION_FILE}: {exc}")
        else:
            st.info(f"`{CALIBRATION_FILE}` not found. Run the calibration below.")

        st.divider()
        days_back = st.slider("Days back", min_value=7, max_value=90, value=14, step=1)
        if st.button("Run Calibration Backtest"):
            progress_placeholder = st.empty()
            progress_placeholder.info("Running calibration — this may take a minute…")
            try:
                # Capture stdout to show inside the app as well
                import sys
                old_stdout = sys.stdout
                sys.stdout = buf = io.StringIO()
                df_result = run_calibration(days_back=days_back)
                sys.stdout = old_stdout
                log_text = buf.getvalue()

                if df_result is not None:
                    progress_placeholder.success("Calibration complete!")
                    if log_text:
                        with st.expander("Console output"):
                            st.text(log_text)
                    st.dataframe(df_result, use_container_width=True)
                    CALIBRATION_PATH  # refresh on next render
                    st.rerun()
                else:
                    sys.stdout = old_stdout
                    progress_placeholder.warning("No data returned. Try increasing 'Days back'.")
            except Exception as exc:
                sys.stdout = old_stdout
                progress_placeholder.error(f"Calibration failed: {exc}")

    # ------------------------------------------------------------------
    # TAB: P&L (collected prices)
    # ------------------------------------------------------------------
    with tab_pnl:
        st.subheader("P&L Backtest (collected prices)")
        st.caption(
            "Matches prices you observed via `--mode collect` against actual settlement results. "
            "Only settled tickers are included; unsettled ones are skipped."
        )

        if PRICE_HISTORY_PATH.exists():
            try:
                df_ph = pd.read_csv(PRICE_HISTORY_PATH)
                st.markdown(f"**Price history rows:** {len(df_ph)}")
                st.dataframe(df_ph, use_container_width=True)
            except Exception as exc:
                st.error(f"Could not load {PRICE_HISTORY_FILE}: {exc}")
        else:
            st.info(f"`{PRICE_HISTORY_FILE}` not found. Run `python backtest.py --mode collect` daily.")

        st.divider()
        if st.button("Run P&L Backtest (requires Kalshi API)"):
            prog = st.empty()
            prog.info("Fetching settlement results — this may take a while…")
            try:
                import sys
                old_stdout = sys.stdout
                sys.stdout = buf = io.StringIO()
                df_result = run_pnl_backtest()
                sys.stdout = old_stdout
                log_text = buf.getvalue()

                if df_result is not None:
                    prog.success("P&L backtest complete!")

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Settled trades", len(df_result))
                    c2.metric("Win rate", f"{df_result['won'].mean():.1%}")
                    c3.metric("Total P&L", f"${df_result['pnl'].sum():.2f}")
                    total_cost = df_result['cost'].sum()
                    roi = df_result['pnl'].sum() / total_cost * 100 if total_cost else 0
                    c4.metric("ROI", f"{roi:.1f}%")

                    if log_text:
                        with st.expander("Console output"):
                            st.text(log_text)
                    st.dataframe(df_result, use_container_width=True)
                else:
                    sys.stdout = old_stdout
                    prog.warning("No settled results yet.")
            except Exception as exc:
                sys.stdout = old_stdout
                prog.error(f"P&L backtest failed: {exc}")

    # ------------------------------------------------------------------
    # TAB: Theoretical P&L
    # ------------------------------------------------------------------
    with tab_theo:
        st.subheader("Theoretical P&L")
        st.caption(
            "Joins calibration signal correctness with price-history entry prices (as a proxy) "
            "to estimate P&L without needing live settlement data."
        )

        if THEORETICAL_PATH.exists():
            try:
                df_theo = pd.read_csv(THEORETICAL_PATH)
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total trades", len(df_theo))
                c2.metric("Win rate", f"{df_theo['signal_correct'].mean():.1%}")
                c3.metric("Total P&L", f"${df_theo['pnl'].sum():.2f}")
                total_cost = df_theo['cost'].sum()
                roi = df_theo['pnl'].sum() / total_cost * 100 if total_cost else 0
                c4.metric("ROI", f"{roi:.1f}%")
                st.dataframe(df_theo, use_container_width=True)
            except Exception as exc:
                st.error(f"Could not load theoretical_pnl.csv: {exc}")
        else:
            st.info("`theoretical_pnl.csv` not found. Run the theoretical P&L computation below.")

        st.divider()
        if st.button("Run Theoretical P&L"):
            prog = st.empty()
            prog.info("Computing theoretical P&L…")
            try:
                import sys
                old_stdout = sys.stdout
                sys.stdout = buf = io.StringIO()
                df_result = run_theoretical_pnl()
                sys.stdout = old_stdout
                log_text = buf.getvalue()

                if df_result is not None:
                    prog.success("Done!")

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Total trades", len(df_result))
                    c2.metric("Win rate", f"{df_result['signal_correct'].mean():.1%}")
                    c3.metric("Total P&L", f"${df_result['pnl'].sum():.2f}")
                    total_cost = df_result['cost'].sum()
                    roi = df_result['pnl'].sum() / total_cost * 100 if total_cost else 0
                    c4.metric("ROI", f"{roi:.1f}%")

                    if log_text:
                        with st.expander("Console output"):
                            st.text(log_text)
                    st.dataframe(df_result, use_container_width=True)
                else:
                    sys.stdout = old_stdout
                    prog.warning(
                        "No results. Make sure both `backtest_calibration.csv` and "
                        "`price_history.csv` exist and share at least one series."
                    )
            except Exception as exc:
                sys.stdout = old_stdout
                prog.error(f"Theoretical P&L failed: {exc}")


# ---------------------------------------------------------------------------
# PAGE: Config
# ---------------------------------------------------------------------------

elif page == "Config":
    st.title("Configuration")

    st.subheader("Trading Parameters")
    st.caption("Changes are written back to `config.py` using regex replacement. Restart the app (or clear cache) for the new values to take effect in signal computations.")

    # Read current values directly from the module (already imported)
    cur_min_edge          = _config_module.MIN_EDGE
    cur_max_pos_pct       = _config_module.MAX_POSITION_PCT
    cur_min_contracts     = _config_module.MIN_CONTRACTS
    cur_max_contracts     = _config_module.MAX_CONTRACTS
    cur_min_hours         = _config_module.MIN_HOURS_TO_CLOSE

    col_left, col_right = st.columns(2)

    with col_left:
        new_min_edge = st.slider(
            "MIN_EDGE  (minimum forecast edge to trade)",
            min_value=0.01, max_value=0.30, value=float(cur_min_edge),
            step=0.01, format="%.2f",
        )
        new_max_pos_pct = st.slider(
            "MAX_POSITION_PCT  (max % of balance per trade)",
            min_value=0.01, max_value=0.50, value=float(cur_max_pos_pct),
            step=0.01, format="%.2f",
        )
        new_min_hours = st.number_input(
            "MIN_HOURS_TO_CLOSE  (min hours before market closes)",
            min_value=0, max_value=48, value=int(cur_min_hours), step=1,
        )

    with col_right:
        new_min_contracts = st.number_input(
            "MIN_CONTRACTS", min_value=1, max_value=100, value=int(cur_min_contracts), step=1,
        )
        new_max_contracts = st.number_input(
            "MAX_CONTRACTS", min_value=1, max_value=500, value=int(cur_max_contracts), step=1,
        )

    if st.button("Save Config"):
        config_path = Path(__file__).parent / "config.py"
        try:
            text = config_path.read_text(encoding="utf-8")

            def _replace_float(pattern_name: str, new_val: float, src: str) -> str:
                return re.sub(
                    rf"^({re.escape(pattern_name)}\s*=\s*)[\d.]+",
                    lambda m: m.group(1) + str(round(new_val, 4)),
                    src,
                    flags=re.MULTILINE,
                )

            def _replace_int(pattern_name: str, new_val: int, src: str) -> str:
                return re.sub(
                    rf"^({re.escape(pattern_name)}\s*=\s*)\d+",
                    lambda m: m.group(1) + str(new_val),
                    src,
                    flags=re.MULTILINE,
                )

            text = _replace_float("MIN_EDGE",          new_min_edge,      text)
            text = _replace_float("MAX_POSITION_PCT",  new_max_pos_pct,   text)
            text = _replace_int(  "MIN_HOURS_TO_CLOSE",new_min_hours,     text)
            text = _replace_int(  "MIN_CONTRACTS",     new_min_contracts, text)
            text = _replace_int(  "MAX_CONTRACTS",     new_max_contracts, text)

            config_path.write_text(text, encoding="utf-8")
            st.success(
                "config.py updated. Restart the Streamlit app (or press R in the terminal) "
                "for new values to take effect."
            )
        except Exception as exc:
            st.error(f"Failed to write config.py: {exc}")

    st.divider()

    # --- Current NOAA forecasts ---
    st.subheader("Current NOAA Forecasts")
    st.caption("Fetched from api.weather.gov. Cached for 5 minutes.")

    if st.button("Refresh Forecasts"):
        cached_noaa_forecasts.clear()
        st.rerun()

    try:
        forecasts = cached_noaa_forecasts()
        rows = []
        for city, fc in sorted(forecasts.items()):
            if fc is None:
                rows.append({"city": city, "status": "no data", "temp_F": "—", "wind": "—", "short_forecast": "—"})
            elif "error" in fc:
                rows.append({"city": city, "status": f"error: {fc['error']}", "temp_F": "—", "wind": "—", "short_forecast": "—"})
            else:
                rows.append({
                    "city":           city,
                    "status":         "ok",
                    "temp_F":         fc.get("temperature", "—"),
                    "wind":           fc.get("windSpeed", "—"),
                    "short_forecast": fc.get("shortForecast", "—"),
                })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        else:
            st.info("No forecast data available.")
    except Exception as exc:
        st.error(f"Could not fetch NOAA forecasts: {exc}")
