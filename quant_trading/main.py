# main.py

import yfinance as yf
import pandas as pd
import numpy as np
from config import TICKERS, INTERVAL, LOOKBACK_DAYS, GAP_THRESHOLD, HOLDING_PERIOD_HOURS
from strategies.opening_reversal import calculate_signals
from backtest.backtest_runner import backtest
import matplotlib.pyplot as plt

def fetch_data(ticker, interval='1m', lookback_days=5):
    df = yf.download(ticker, interval=interval, period=f'{lookback_days}d')
    df = df.between_time('09:30', '16:00')
    return df

def run_backtest():
    all_returns = []

    for ticker in TICKERS:
        print(f"Fetching data for {ticker}...")
        df = fetch_data(ticker)

        print(f"Calculating signals for {ticker}...")
        signals = calculate_signals(df, gap_threshold=GAP_THRESHOLD, ticker=ticker)

        print(f"Backtesting {ticker}...")
        cumulative_returns, returns = backtest(df, signals, holding_period_hours=HOLDING_PERIOD_HOURS)
        all_returns.append(returns)

        plt.plot(cumulative_returns, label=ticker)

    plt.title("Opening Reversal Strategy")
    plt.xlabel("Trade #")
    plt.ylabel("Cumulative Return")
    plt.legend()
    plt.grid(True)
    plt.show()

    # Aggregate metrics
    total_returns = pd.concat(all_returns)
    # Calculate performance metrics
    total_cumulative_return = (1 + total_returns.fillna(0)).prod() - 1
    sharpe_ratio = total_returns.mean() / total_returns.std() * np.sqrt(252 * 6.5)  # 252 trading days × 6.5 hours
    win_rate = (total_returns > 0).mean()

    print("\n--- Strategy Performance Summary ---")
    print(f"Total Cumulative Return: {total_cumulative_return:.2%}")
    print(f"Sharpe Ratio: {sharpe_ratio:.2f}")
    print(f"Win Rate: {win_rate:.2%}")
    print(f"Number of Trades: {len(total_returns)}")
    print("-------------------------------------")

if __name__ == "__main__":
    run_backtest()
