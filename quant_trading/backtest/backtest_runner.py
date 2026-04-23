# backtest/backtest_runner.py

import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta

def backtest(df, signals, holding_period_hours=2):
    df['date'] = df.index.date
    df = df.reset_index(drop=True)
    df.columns = [col[0] for col in df.columns]


    # Merge signals
    df = df.merge(signals[['date', 'signal']], on='date', how='left')
    df['signal'].fillna(0, inplace=True)

    returns = []
    position = 0
    entry_price = None
    exit_time = None

    for idx, row in df.iterrows():
        current_time = row['time']

        # Force close position at exactly 4:00 PM
        if current_time >= time(16, 0) and position != 0:
            exit_price = row['Close']
            print(f"Force closing trade at 16:00 for {exit_price}")
            if position == 1:
                returns.append((exit_price - entry_price) / entry_price)
            else:
                returns.append((entry_price - exit_price) / entry_price)
            position = 0
            entry_price = None
            exit_time = None
            continue  # Important! Go to next loop after forced close

        # Open new trade if not in position
        if position == 0:
            if row['signal'] == 1:
                position = 1
                entry_price = row['Close']
                entry_datetime = datetime.combine(datetime.today(), current_time)
                exit_datetime = entry_datetime + timedelta(hours=holding_period_hours)
                exit_time = min(exit_datetime.time(), time(16, 0))
                print(f"Opened LONG at {current_time}, exit scheduled at {exit_time}")
            elif row['signal'] == -1:
                position = -1
                entry_price = row['Close']
                entry_datetime = datetime.combine(datetime.today(), current_time)
                exit_datetime = entry_datetime + timedelta(hours=holding_period_hours)
                exit_time = min(exit_datetime.time(), time(16, 0))
                print(f"Opened SHORT at {current_time}, exit scheduled at {exit_time}")

        # If already in position, check if time to exit
        elif position != 0:
            if current_time >= exit_time:
                exit_price = row['Close']
                if position == 1:
                    returns.append((exit_price - entry_price) / entry_price)
                    print(f"Closed LONG at {current_time}")
                else:
                    returns.append((entry_price - exit_price) / entry_price)
                    print(f"Closed SHORT at {current_time}")
                position = 0
                entry_price = None
                exit_time = None

    return (1 + pd.Series(returns).fillna(0)).cumprod(), pd.Series(returns)
