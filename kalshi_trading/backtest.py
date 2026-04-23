"""
Backtest the weather strategy against historical Kalshi markets.

TWO MODES:

1. Calibration backtest (--mode calibrate)
   Uses actual NOAA observations as a perfect forecast, checks whether our
   signal direction (BUY YES / BUY NO) matches the settlement result.
   Cannot compute real P&L because historical entry prices aren't available via API.

2. Price collection (--mode collect)
   Records today's open market prices to CSV so that in a week or two you can
   run a proper P&L backtest against prices you actually observed at entry.

Usage:
    python backtest.py                        # calibration, last 14 days
    python backtest.py --mode calibrate --days 30
    python backtest.py --mode collect         # run daily to build price history
"""

import argparse
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

from client import KalshiClient
from config import WEATHER_SERIES, MIN_EDGE, MAX_POSITION_PCT, MIN_CONTRACTS, MAX_CONTRACTS
from strategy import (
    SERIES_TO_CITY, _parse_market_type, _parse_event_date,
    forecast_probability, get_signals, get_noaa_forecast
)

# NWS station IDs matching Kalshi's settlement stations
CITY_STATION = {
    "NYC": "KNYC",   # Central Park
    "MIA": "KMIA",   # Miami International Airport
    "DAL": "KDAL",   # Dallas Love Field
    "DC":  "KDCA",   # Reagan National
}

PRICE_HISTORY_FILE = "price_history.csv"


# ---------------------------------------------------------------------------
# NOAA historical observations
# ---------------------------------------------------------------------------

def get_observed_high(city: str, date) -> float | None:
    """Fetch the actual recorded high temp for a city on a given date from NWS."""
    station = CITY_STATION.get(city)
    if not station:
        return None
    url = f"https://api.weather.gov/stations/{station}/observations"
    try:
        resp = requests.get(
            url,
            params={"start": f"{date}T00:00:00Z", "end": f"{date}T23:59:59Z"},
            headers={"User-Agent": "kalshi-backtest"},
            timeout=15,
        )
        resp.raise_for_status()
        temps = []
        for obs in resp.json().get("features", []):
            val = obs.get("properties", {}).get("temperature", {}).get("value")
            if val is not None:
                temps.append(val * 9 / 5 + 32)  # °C → °F
        return round(max(temps), 1) if temps else None
    except Exception as e:
        print(f"  Observation fetch failed for {city} on {date}: {e}")
        return None


# ---------------------------------------------------------------------------
# Fetch settled markets
# ---------------------------------------------------------------------------

def fetch_settled_markets(client: KalshiClient, series: str, days_back: int) -> list[dict]:
    """Return finalized markets for a series within the past N days."""
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=days_back)
    all_markets = []

    try:
        events = client._get("/events", {"series_ticker": series, "limit": days_back + 5}).get("events", [])
        time.sleep(0.3)
        for event in events:
            event_date = _parse_event_date(event.get("event_ticker", ""))
            if event_date is None or not (cutoff <= event_date < today):
                continue
            markets = client._get("/markets", {"event_ticker": event["event_ticker"], "limit": 20}).get("markets", [])
            settled = [m for m in markets if m.get("status") == "finalized" and m.get("result")]
            for m in settled:
                m["series_ticker"] = series
            all_markets.extend(settled)
            time.sleep(0.3)
    except Exception as e:
        print(f"  Error fetching {series}: {e}")

    return all_markets


# ---------------------------------------------------------------------------
# Calibration backtest
# ---------------------------------------------------------------------------

def run_calibration(days_back: int = 14):
    """
    For each settled market, check whether our model (using actual observed temp
    as a perfect forecast) would have called the right direction.
    """
    client = KalshiClient()
    rows = []

    for series in WEATHER_SERIES:
        city = SERIES_TO_CITY.get(series)
        if not city:
            continue

        print(f"\n{series} ({city})")
        markets = fetch_settled_markets(client, series, days_back)
        print(f"  {len(markets)} settled markets")

        by_date = {}
        for m in markets:
            d = _parse_event_date(m.get("ticker", ""))
            if d:
                by_date.setdefault(d, []).append(m)

        for event_date, date_markets in sorted(by_date.items()):
            actual_high = get_observed_high(city, event_date)
            if actual_high is None:
                print(f"  {event_date}: no observation")
                time.sleep(0.5)
                continue

            print(f"  {event_date}: actual high = {actual_high}°F")

            for market in date_markets:
                synthetic_forecast = {"temperature": actual_high}
                prob = forecast_probability(market, synthetic_forecast)
                if prob is None:
                    continue

                result = market.get("result", "").lower()
                market_type, threshold = _parse_market_type(market)

                # Would we have signalled YES or NO?
                # Use 0.5 as midpoint since we don't have actual entry prices
                yes_edge = prob - 0.5
                no_edge  = (1 - prob) - 0.5
                if yes_edge >= MIN_EDGE:
                    signal_side = "yes"
                elif no_edge >= MIN_EDGE:
                    signal_side = "no"
                else:
                    signal_side = None

                rows.append({
                    "ticker":       market["ticker"],
                    "event_date":   str(event_date),
                    "series":       series,
                    "market_type":  market_type,
                    "threshold":    threshold,
                    "actual_high":  actual_high,
                    "our_prob":     round(prob, 3),
                    "result":       result,
                    "signal_side":  signal_side,
                    "signal_correct": (signal_side == result) if signal_side else None,
                })

            time.sleep(0.3)

    if not rows:
        print("\nNo data. Try --days 30 or check that NOAA station IDs are correct.")
        return None

    df = pd.DataFrame(rows)
    signalled = df[df["signal_side"].notna()]

    print("\n" + "=" * 60)
    print("CALIBRATION RESULTS")
    print("=" * 60)
    print(f"Period:           last {days_back} days")
    print(f"Total markets:    {len(df)}")
    print(f"Markets signalled:{len(signalled)}  ({len(signalled)/len(df):.0%} of all)")
    if len(signalled):
        print(f"Direction correct:{signalled['signal_correct'].mean():.1%}")
        print()
        print("By series:")
        print(signalled.groupby("series")[["signal_correct"]].mean().round(3).to_string())
        print()
        print("By market type:")
        print(signalled.groupby("market_type")[["signal_correct"]].agg(["mean","count"]).round(3).to_string())

    df.to_csv("backtest_calibration.csv", index=False)
    print("\nFull results saved to backtest_calibration.csv")
    return df


# ---------------------------------------------------------------------------
# Price collector — run daily to build up entry price history
# ---------------------------------------------------------------------------

def collect_prices():
    """
    Snapshot today's open market prices and forecasts.
    Append to price_history.csv so we can later match against settlement results.
    """
    client = KalshiClient()
    from markets import get_open_weather_markets
    markets = get_open_weather_markets(client)
    balance = client.get_balance().get("balance", 0) / 100
    signals = get_signals(markets, balance)

    if not signals:
        print("No signals today — nothing to record.")
        return

    today = datetime.now(timezone.utc).date()
    rows = []
    for s in signals:
        rows.append({
            "snapshot_date":  str(today),
            "ticker":         s["ticker"],
            "series":         s["series"],
            "city":           s["city"],
            "side":           s["side"],
            "entry_price":    s["price"],
            "forecast_prob":  s["forecast_prob"],
            "edge":           s["edge"],
            "contracts":      s["contracts"],
            "strike":         s.get("strike"),
            "forecast_temp":  s.get("forecast_temp"),
        })

    df_new = pd.DataFrame(rows)
    path = Path(PRICE_HISTORY_FILE)
    if path.exists():
        df_existing = pd.read_csv(path)
        df_out = pd.concat([df_existing, df_new], ignore_index=True).drop_duplicates(
            subset=["snapshot_date", "ticker", "side"]
        )
    else:
        df_out = df_new

    df_out.to_csv(path, index=False)
    print(f"Recorded {len(rows)} signals to {PRICE_HISTORY_FILE}")
    print(df_new[["ticker","side","entry_price","forecast_prob","edge"]].to_string(index=False))


# ---------------------------------------------------------------------------
# P&L backtest against collected price history
# ---------------------------------------------------------------------------

def run_pnl_backtest():
    """
    Once you have collected prices for a week+, match them against
    settlement results to compute actual P&L.

    Skips tickers that are not yet settled and prints whatever is settled
    so far — useful for partial history.
    """
    path = Path(PRICE_HISTORY_FILE)
    if not path.exists():
        print(f"{PRICE_HISTORY_FILE} not found — run --mode collect daily first.")
        return

    client = KalshiClient()
    df = pd.read_csv(path)
    results = []
    skipped_unsettled = 0

    for _, row in df.iterrows():
        try:
            market = client._get(f"/markets/{row['ticker']}").get("market", {})
            result = market.get("result", "").lower()
            if not result:
                skipped_unsettled += 1
                continue  # not yet settled — skip but keep going

            won = result == row["side"]
            cost   = row["contracts"] * row["entry_price"]
            payout = row["contracts"] * 1.0 if won else 0.0
            results.append({**row.to_dict(), "result": result, "won": won,
                             "cost": cost, "payout": payout, "pnl": payout - cost})
        except Exception:
            pass
        time.sleep(0.2)

    if skipped_unsettled:
        print(f"  (skipped {skipped_unsettled} unsettled markets)")

    if not results:
        print("No settled markets found yet in price history.")
        return None

    df_r = pd.DataFrame(results)
    total_cost = df_r["cost"].sum()
    total_pnl  = df_r["pnl"].sum()
    roi = (total_pnl / total_cost * 100) if total_cost > 0 else 0.0

    print(f"\nP&L BACKTEST — {len(df_r)} settled trades")
    print(f"Win rate:  {df_r['won'].mean():.1%}")
    print(f"Total P&L: ${total_pnl:.2f}")
    print(f"Total cost:${total_cost:.2f}")
    print(f"ROI:       {roi:.1f}%")
    print()
    print(df_r[["ticker","side","entry_price","result","won","cost","pnl"]].to_string(index=False))
    return df_r


# ---------------------------------------------------------------------------
# Theoretical P&L using calibration + price history as price proxy
# ---------------------------------------------------------------------------

CALIBRATION_FILE = "backtest_calibration.csv"


def run_theoretical_pnl():
    """
    Estimate P&L by joining calibration results (signal direction + correctness)
    with price_history.csv (used as a proxy for typical entry prices).

    The join is on (series) so that any price observation for the same market
    series provides a realistic entry-price proxy even when dates don't align
    exactly.  Rows in the calibration data that have no price proxy or no
    signal are skipped.

    Steps
    -----
    1. Load backtest_calibration.csv  (produced by run_calibration())
    2. Load price_history.csv         (produced by collect_prices())
    3. Join on series, using the median entry_price per (series, side) as proxy
    4. For each calibration row with a signal:
         cost   = contracts * entry_price_proxy
         payout = contracts * 1.0  if signal_correct else 0.0
         pnl    = payout - cost
    5. Print summary + per-row table
    """
    cal_path = Path(CALIBRATION_FILE)
    ph_path  = Path(PRICE_HISTORY_FILE)

    if not cal_path.exists():
        print(f"{CALIBRATION_FILE} not found — run --mode calibrate first.")
        return None

    if not ph_path.exists():
        print(f"{PRICE_HISTORY_FILE} not found — run --mode collect daily first.")
        return None

    df_cal = pd.read_csv(cal_path)
    df_ph  = pd.read_csv(ph_path)

    # Keep only calibration rows where we had a signal and know whether it was correct
    df_cal = df_cal[df_cal["signal_side"].notna() & df_cal["signal_correct"].notna()].copy()

    if df_cal.empty:
        print("No signalled rows in calibration data.")
        return None

    # Build a price proxy table: median entry_price per (series, side) from price history
    proxy = (
        df_ph.groupby(["series", "side"])["entry_price"]
        .median()
        .reset_index()
        .rename(columns={"entry_price": "entry_price_proxy", "side": "signal_side"})
    )

    # Merge calibration rows with price proxy on (series, signal_side)
    df_merged = df_cal.merge(proxy, on=["series", "signal_side"], how="inner")

    if df_merged.empty:
        print(
            "No rows survived the join between calibration and price history.\n"
            "Make sure both files share at least one series ticker."
        )
        return None

    # Use the median contracts from price history as a stand-in for position size
    contracts_proxy = (
        df_ph.groupby(["series", "side"])["contracts"]
        .median()
        .reset_index()
        .rename(columns={"contracts": "contracts_proxy", "side": "signal_side"})
    )
    df_merged = df_merged.merge(contracts_proxy, on=["series", "signal_side"], how="left")
    df_merged["contracts_proxy"] = df_merged["contracts_proxy"].fillna(1)

    # Compute P&L
    df_merged["cost"]   = df_merged["contracts_proxy"] * df_merged["entry_price_proxy"]
    df_merged["payout"] = df_merged.apply(
        lambda r: r["contracts_proxy"] * 1.0 if r["signal_correct"] else 0.0, axis=1
    )
    df_merged["pnl"] = df_merged["payout"] - df_merged["cost"]

    total_trades = len(df_merged)
    win_rate     = df_merged["signal_correct"].mean()
    total_cost   = df_merged["cost"].sum()
    total_pnl    = df_merged["pnl"].sum()
    roi          = (total_pnl / total_cost * 100) if total_cost > 0 else 0.0

    print("\n" + "=" * 60)
    print("THEORETICAL P&L BACKTEST")
    print("=" * 60)
    print(f"Total trades:   {total_trades}")
    print(f"Win rate:       {win_rate:.1%}")
    print(f"Total P&L:      ${total_pnl:.2f}")
    print(f"Total cost:     ${total_cost:.2f}")
    print(f"ROI:            {roi:.1f}%")
    print()
    display_cols = [
        "ticker", "event_date", "series", "signal_side", "signal_correct",
        "entry_price_proxy", "contracts_proxy", "cost", "pnl",
    ]
    print(df_merged[display_cols].to_string(index=False))

    out_path = Path("theoretical_pnl.csv")
    df_merged.to_csv(out_path, index=False)
    print(f"\nFull results saved to {out_path}")
    return df_merged


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["calibrate", "collect", "pnl", "theoretical"],
        default="calibrate",
    )
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()

    if args.mode == "calibrate":
        run_calibration(days_back=args.days)
    elif args.mode == "collect":
        collect_prices()
    elif args.mode == "pnl":
        run_pnl_backtest()
    elif args.mode == "theoretical":
        run_theoretical_pnl()
