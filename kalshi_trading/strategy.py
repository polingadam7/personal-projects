"""
Weather event prediction strategy.

Fetches NOAA forecasts for tracked cities, compares to Kalshi market prices,
and returns trade signals where the edge exceeds the configured threshold.
"""

import math
import re
from datetime import datetime, timezone, timedelta
import requests

from config import MIN_EDGE, MAX_CONTRACTS, MIN_CONTRACTS, MAX_POSITION_PCT, MIN_HOURS_TO_CLOSE

# NOAA gridpoint forecast endpoints — find yours at https://api.weather.gov/points/{lat},{lon}
CITY_NOAA = {
    "NYC":   "https://api.weather.gov/gridpoints/OKX/34,45/forecast",   # Central Park
    "MIA":   "https://api.weather.gov/gridpoints/MFL/106,51/forecast",  # Miami Intl Airport
    "DAL":   "https://api.weather.gov/gridpoints/FWD/87,107/forecast",  # Dallas Love Field
    "DC":    "https://api.weather.gov/gridpoints/LWX/97,69/forecast",   # Reagan National
}

# Map series tickers to city keys
SERIES_TO_CITY = {
    "KXHIGHNY":   "NYC",
    "KXRAINNYC":  "NYC",
    "KXHIGHMIA":  "MIA",
    "KXHIGHTDAL": "DAL",
    "KXHIGHTDC":  "DC",
}

# Assumed forecast uncertainty (std dev in °F)
TEMP_SIGMA = 5.0


def get_noaa_forecast(city: str, target_date=None) -> dict | None:
    """
    Fetch the daytime forecast period for target_date (defaults to tomorrow).
    We default to tomorrow because we only trade next-day markets.
    """
    if target_date is None:
        target_date = (datetime.now(timezone.utc) + timedelta(days=1)).date()

    url = CITY_NOAA.get(city)
    if not url:
        return None
    try:
        resp = requests.get(url, headers={"User-Agent": "kalshi-weather-bot"}, timeout=10)
        resp.raise_for_status()
        for period in resp.json()["properties"]["periods"]:
            if not period.get("isDaytime"):
                continue
            period_date = datetime.fromisoformat(period["startTime"]).date()
            if period_date == target_date:
                return period
    except Exception as e:
        print(f"NOAA fetch failed for {city}: {e}")
    return None


BRACKET_WIDTH = 2.0  # °F — each bracket market covers a 2°F range


def _parse_event_date(ticker: str):
    """
    Extract the event date from a ticker like KXHIGHNY-26APR23-T68.
    Returns a datetime.date or None if it can't be parsed.
    """
    parts = ticker.split("-")
    if len(parts) < 2:
        return None
    try:
        return datetime.strptime(parts[1], "%y%b%d").date()
    except ValueError:
        return None


def _normal_cdf(z: float) -> float:
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def _parse_market_type(market: dict) -> tuple[str, float | None]:
    """
    Returns (market_type, threshold) where market_type is one of:
      'greater'  — resolves YES if high > threshold  (e.g. T67 with floor_strike set)
      'less'     — resolves YES if high < threshold  (e.g. T60 with no floor_strike)
      'bracket'  — resolves YES if threshold <= high < threshold + BRACKET_WIDTH  (e.g. B64.5)
    Threshold is in °F.
    """
    ticker = market.get("ticker", "")
    floor_strike = market.get("floor_strike")

    # Extract the suffix after the date portion, e.g. "B64.5" or "T67"
    parts = ticker.split("-")
    suffix = parts[-1] if parts else ""

    if suffix.startswith("B"):
        return "bracket", float(floor_strike) if floor_strike is not None else None

    if suffix.startswith("T"):
        if floor_strike is not None:
            return "greater", float(floor_strike)
        else:
            # Less-than market — parse threshold from the ticker suffix (e.g. T60 → 60)
            try:
                threshold = float(suffix[1:])
                return "less", threshold
            except ValueError:
                return "unknown", None

    return "unknown", None


def forecast_probability(market: dict, forecast: dict) -> float | None:
    """
    Estimate the probability that a market resolves YES given a NOAA forecast.
    Correctly handles greater-than, less-than, and bracket temperature markets, plus rain.
    """
    series = market.get("series_ticker", "")
    title = market.get("title", "").lower()

    # --- Rain market ---
    if "rain" in series.lower() or "rain" in title:
        pop = forecast.get("probabilityOfPrecipitation", {})
        val = pop.get("value") if isinstance(pop, dict) else pop
        if val is None:
            detail = forecast.get("detailedForecast", "").lower()
            return 0.4 if ("chance of rain" in detail or "showers" in detail) else None
        return float(val) / 100.0

    # --- High temp market ---
    if "high" in series.lower() or "high" in title:
        forecast_high = forecast.get("temperature")
        if forecast_high is None:
            return None
        forecast_high = float(forecast_high)

        market_type, threshold = _parse_market_type(market)
        if threshold is None:
            return None

        sigma = TEMP_SIGMA
        if market_type == "greater":
            # P(high > threshold)
            return 1.0 - _normal_cdf((threshold - forecast_high) / sigma)
        elif market_type == "less":
            # P(high < threshold)
            return _normal_cdf((threshold - forecast_high) / sigma)
        elif market_type == "bracket":
            # P(threshold <= high < threshold + BRACKET_WIDTH)
            p_above_lower = 1.0 - _normal_cdf((threshold - forecast_high) / sigma)
            p_above_upper = 1.0 - _normal_cdf((threshold + BRACKET_WIDTH - forecast_high) / sigma)
            return p_above_lower - p_above_upper

    return None


def dollars_to_prob(price_str: str) -> float:
    """Convert a price string like '0.3500' to a probability float 0.35."""
    try:
        return float(price_str)
    except (TypeError, ValueError):
        return None


def get_signals(markets: list[dict], balance_dollars: float) -> list[dict]:
    """
    Compare NOAA forecasts to Kalshi market prices.
    Returns trade signals with ticker, side, price, and contracts.
    balance_dollars: current account balance in dollars.
    """
    signals = []
    forecasts = {}

    now = datetime.now(timezone.utc)
    tomorrow = (now + timedelta(days=1)).date()

    for market in markets:
        close_time_str = market.get("close_time")
        if not close_time_str:
            continue
        close_time = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))

        # Derive the event date from the ticker (e.g. KXHIGHNY-26APR23-T68 → Apr 23 2026)
        # More reliable than close_time, which is often the following midnight
        event_date = _parse_event_date(market.get("ticker", ""))
        if event_date is None:
            continue

        # Only trade markets for tomorrow
        if event_date != tomorrow:
            continue

        # Require minimum time before close
        if (close_time - now) < timedelta(hours=MIN_HOURS_TO_CLOSE):
            continue

        series = market.get("series_ticker", "")
        city = SERIES_TO_CITY.get(series)
        if not city:
            continue

        if city not in forecasts:
            forecasts[city] = get_noaa_forecast(city, target_date=event_date)

        forecast = forecasts.get(city)
        if not forecast:
            continue

        forecast_prob = forecast_probability(market, forecast)
        if forecast_prob is None:
            continue

        yes_ask = dollars_to_prob(market.get("yes_ask_dollars"))
        yes_bid = dollars_to_prob(market.get("yes_bid_dollars"))
        if yes_ask is None or yes_bid is None:
            continue

        # Skip illiquid markets (no bid/ask)
        if yes_ask == 0 and yes_bid == 0:
            continue

        # Skip effectively settled markets — price has moved to near certainty
        if yes_ask >= 0.95 or yes_bid >= 0.95:
            continue

        no_ask = 1.0 - yes_bid  # cost to buy NO

        yes_edge = forecast_prob - yes_ask
        no_edge = (1.0 - forecast_prob) - no_ask

        signal = None
        if yes_edge >= MIN_EDGE:
            signal = {"side": "yes", "price": yes_ask, "edge": yes_edge}
        elif no_edge >= MIN_EDGE:
            signal = {"side": "no", "price": no_ask, "edge": no_edge}

        if signal:
            max_spend = balance_dollars * MAX_POSITION_PCT
            contracts = int(max_spend / signal["price"]) if signal["price"] > 0 else 0
            contracts = max(MIN_CONTRACTS, min(MAX_CONTRACTS, contracts))

            signal.update({
                "ticker": market["ticker"],
                "contracts": contracts,
                "forecast_prob": round(forecast_prob, 3),
                "market_price": round(signal["price"], 3),
                "price_cents": round(signal["price"] * 100),
                "series": series,
                "city": city,
                "strike": market.get("floor_strike"),
                "forecast_temp": forecast.get("temperature"),
            })
            signals.append(signal)
            print(
                f"  SIGNAL {signal['ticker']} | BUY {signal['side'].upper()} @ {signal['price']:.2f} "
                f"| forecast={signal['forecast_prob']:.1%} market={signal['market_price']:.1%} "
                f"edge={signal['edge']:.1%} | {contracts} contracts"
            )

    return signals
