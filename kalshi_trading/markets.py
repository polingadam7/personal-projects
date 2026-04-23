"""Utilities for finding and inspecting weather markets on Kalshi."""

import time

from client import KalshiClient
from config import WEATHER_SERIES


def get_open_weather_markets(client: KalshiClient) -> list[dict]:
    """
    Return all open markets for configured weather series.
    Each series has events (one per day), each event has multiple markets (one per threshold).
    """
    all_markets = []

    for series in WEATHER_SERIES:
        try:
            result = client._get("/events", {"series_ticker": series, "status": "open", "limit": 10})
            events = result.get("events", [])
        except Exception as e:
            print(f"Could not fetch events for {series}: {e}")
            time.sleep(1)
            continue

        for event in events:
            event_ticker = event["event_ticker"]
            try:
                markets = client.get_markets(event_ticker=event_ticker, status="open")
                for m in markets:
                    m["series_ticker"] = series  # attach series for strategy lookup
                all_markets.extend(markets)
            except Exception as e:
                print(f"Could not fetch markets for {event_ticker}: {e}")

            time.sleep(0.3)

        time.sleep(0.5)

    return all_markets


def print_market_summary(market: dict):
    ticker = market.get("ticker", "")
    title = market.get("title", "").replace("**", "")
    yes_bid = market.get("yes_bid_dollars", "?")
    yes_ask = market.get("yes_ask_dollars", "?")
    strike = market.get("floor_strike", "?")
    print(f"{ticker:45s} | strike={strike:>4} | YES {yes_bid}/{yes_ask} | {title[:50]}")


if __name__ == "__main__":
    client = KalshiClient()
    markets = get_open_weather_markets(client)

    if not markets:
        print("No open weather markets found.")
    else:
        print(f"\nFound {len(markets)} open weather markets:\n")
        print(f"{'Ticker':45s} | Strike | YES bid/ask | Title")
        print("-" * 110)
        for m in sorted(markets, key=lambda x: x.get("ticker", "")):
            print_market_summary(m)
