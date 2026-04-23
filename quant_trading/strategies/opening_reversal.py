# strategies/opening_reversal.py

import pandas as pd
import numpy as np

def calculate_signals(df, gap_threshold=0.02, ticker='SPY'):
    # Prepare
    df['date'] = df.index.date
    df['time'] = df.index.time

    # Open and close prices
    opens = df.groupby('date').first()
    closes = df.groupby('date').last()

    # Gap calculation
    gap_pct = (opens['Open'] - closes['Close'].shift(1)) / closes['Close'].shift(1)

    # 15-minute return after open
    fifteen_min_return = (
        df.groupby('date').nth(15)
          .Close.pct_change()
    )

    signals = pd.DataFrame({
        'date': gap_pct.reset_index()['date'], 
        'gap_pct': gap_pct.reset_index()[ticker],
        '15min_return': fifteen_min_return.reset_index()[ticker]
    }).dropna()

    # Generate trade signals
    def generate_trade(row):
        combined = row['gap_pct'] + row['15min_return']
        if combined > gap_threshold:
            return -1  # Short
        elif combined < -gap_threshold:
            return 1  # Long
        else:
            return 0  # No trade

    signals['signal'] = signals.apply(generate_trade, axis=1)

    if len(gap_pct) < 2 or len(fifteen_min_return) < 2:
        print("Not enough historical days to calculate signals. Skipping.")
        return pd.DataFrame()  # Return empty DataFrame safely
    
    return signals 
