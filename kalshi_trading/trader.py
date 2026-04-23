"""
Live trading bot. Runs on a loop, checks for signals, and places orders.

Usage:
    python trader.py           # dry run (no real orders)
    python trader.py --live    # place real orders
"""

import argparse
import time

from client import KalshiClient
from markets import get_open_weather_markets
from strategy import get_signals


POLL_INTERVAL_SECONDS = 300  # check every 5 minutes


def run(live: bool = False):
    client = KalshiClient()

    balance_data = client.get_balance()
    # Balance is returned in cents, convert to dollars
    balance_dollars = balance_data.get("balance", 0) / 100
    print(f"Balance: ${balance_dollars:.2f}")

    if not live:
        print("DRY RUN — no orders will be placed. Pass --live to trade.\n")

    while True:
        print(f"\n--- Scanning markets ---")
        markets = get_open_weather_markets(client)
        print(f"Found {len(markets)} open weather markets")

        signals = get_signals(markets, balance_dollars)

        if not signals:
            print("No signals above threshold.")
        elif not live:
            print(f"{len(signals)} signal(s) found (dry run — not placing orders)")
        else:
            for signal in signals:
                try:
                    order = client.place_order(
                        ticker=signal["ticker"],
                        side=signal["side"],
                        count=signal["contracts"],
                        order_type="limit",
                        yes_price=signal["price_cents"] if signal["side"] == "yes" else None,
                        no_price=signal["price_cents"] if signal["side"] == "no" else None,
                    )
                    print(f"Order placed: {order}")
                except Exception as e:
                    print(f"Order failed for {signal['ticker']}: {e}")

            balance_dollars = client.get_balance().get("balance", balance_dollars * 100) / 100
            print(f"Updated balance: ${balance_dollars:.2f}")

        print(f"Sleeping {POLL_INTERVAL_SECONDS}s...")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Place real orders")
    args = parser.parse_args()
    run(live=args.live)
