import sqlite3
import os
import pandas as pd
import json
import matplotlib.pyplot as plt
import argparse
import numpy as np
from datetime import datetime

def find_tickers(db_path, pattern):
    """Find all tickers in the database matching the given pattern."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Replace wildcard * with SQL wildcard %
    sql_pattern = pattern.replace('*', '%')
    
    print(f"Searching for tickers matching: {sql_pattern}")
    cursor.execute("SELECT DISTINCT ticker FROM trades WHERE ticker LIKE ?", (sql_pattern,))
    tickers = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return tickers

def load_data(db_path, ticker):
    """Load trades and snapshots for a specific ticker."""
    conn = sqlite3.connect(db_path)
    
    print(f"Loading trades for {ticker}...")
    trades_df = pd.read_sql_query(
        "SELECT * FROM trades WHERE ticker = ? ORDER BY timestamp",
        conn,
        params=(ticker,)
    )
    
    print(f"Loading snapshots for {ticker}...")
    snapshots_df = pd.read_sql_query(
        "SELECT * FROM orderbook_snapshots WHERE ticker = ? ORDER BY timestamp",
        conn,
        params=(ticker,)
    )
    
    conn.close()
    return trades_df, snapshots_df

def get_best_prices(snapshot_json):
    """Extract best bid and ask from snapshot JSON."""
    try:
        data = json.loads(snapshot_json)
        # Format is list of [price, size]
        yes_bids = data.get('yes', [])
        # For 'no' side, we need to convert to 'yes' equivalent if we want a unified view,
        # but typically orderbooks are stored as Yes Bids and No Bids (which are effectively Yes Asks).
        # In Kalshi:
        # "Yes" orders are bids for Yes.
        # "No" orders are bids for No.
        # A bid for No at price P is equivalent to an Ask for Yes at price (100 - P).
        
        no_bids = data.get('no', [])
        
        best_bid = 0
        if yes_bids:
            # Assuming sorted or we take max
            best_bid = max([p[0] for p in yes_bids])
            
        best_ask = 100 # Default max
        if no_bids:
            # Best No bid is the highest price someone will pay for No.
            # This corresponds to the lowest price someone will sell Yes for.
            # Yes Price = 100 - No Price.
            # So max(No Bid) -> min(Yes Ask)
            best_no_bid = max([p[0] for p in no_bids])
            best_ask = 100 - best_no_bid
            
        return best_bid, best_ask
    except Exception as e:
        return None, None

def process_data(trades_df, snapshots_df):
    """Process data to calculate slippage and markouts."""
    
    # 1. Process Snapshots to get Mid Prices
    print("Processing snapshots...")
    snapshots_df['best_bid'], snapshots_df['best_ask'] = zip(*snapshots_df['snapshot_data'].map(get_best_prices))
    snapshots_df['mid_price'] = (snapshots_df['best_bid'] + snapshots_df['best_ask']) / 2
    
    # Filter out invalid snapshots
    snapshots_df = snapshots_df.dropna(subset=['mid_price'])
    
    # 2. Process Trades
    print("Processing trades...")
    # Parse trade price and count.
    def get_trade_details(trade_json):
        try:
            data = json.loads(trade_json)
            # data['yes_price'] is often the execution price for the Yes contract
            price = data.get('yes_price', data.get('yes_price_dollars'))
            count = data.get('count', 0)
            return price, count
        except:
            return None, None

    trades_df[['exec_price', 'count']] = trades_df['trade_data'].apply(
        lambda x: pd.Series(get_trade_details(x))
    )
    trades_df = trades_df.dropna(subset=['exec_price'])
    
    # 3. Merge Trades with Snapshots
    # We want the snapshot immediately BEFORE or AT the trade time.
    # Since we have 1s precision, we'll use merge_asof.
    
    # Ensure timestamps are sorted
    trades_df = trades_df.sort_values('timestamp')
    snapshots_df = snapshots_df.sort_values('timestamp')
    
    merged_df = pd.merge_asof(
        trades_df,
        snapshots_df[['timestamp', 'mid_price']],
        on='timestamp',
        direction='backward'
    )
    
    # 4. Determine Side and Calculate Slippage
    # Determine "Side" based on Exec vs Mid at trade time.
    # If Exec > Mid, we assume it was a BUY (taker crossed spread).
    # If Exec < Mid, we assume it was a SELL.
    # If Exec == Mid, it's ambiguous, maybe ignore or treat as neutral.
    
    conditions = [
        merged_df['exec_price'] > merged_df['mid_price'],
        merged_df['exec_price'] < merged_df['mid_price']
    ]
    choices = [1, -1] # 1 for Buy, -1 for Sell
    merged_df['side'] = np.select(conditions, choices, default=0)

    # Slippage = (Execution Price - Mid Price) * Side
    # For Buy (Side 1): Exec - Mid
    # For Sell (Side -1): (Exec - Mid) * -1 = Mid - Exec
    # This ensures slippage represents the cost paid (crossing the spread).
    merged_df['slippage'] = (merged_df['exec_price'] - merged_df['mid_price']) * merged_df['side']
    
    # Calculate Slippage Paid = Slippage * Count
    merged_df['slippage_paid'] = merged_df['slippage'] * merged_df['count']

    # 5. Calculate Markouts
    # We want to see the mid price N seconds AFTER the trade.
    # We can use merge_asof again with shifted timestamps.
    
    intervals = [1, 10, 30, 60, 300] # seconds
    
    for interval in intervals:
        # Create a target timestamp
        merged_df[f'ts_plus_{interval}'] = merged_df['timestamp'] + interval
        
        # Find the mid price at that future time
        # We need a temporary dataframe for the merge
        temp_df = pd.merge_asof(
            merged_df[[f'ts_plus_{interval}']],
            snapshots_df[['timestamp', 'mid_price']],
            left_on=f'ts_plus_{interval}',
            right_on='timestamp',
            direction='backward'
        )
        
        merged_df[f'mid_price_{interval}s'] = temp_df['mid_price']
        
        # Markout (Profit/Loss for the TAKER):
        # For Buy: Future Mid - Exec Price
        # For Sell: Exec Price - Future Mid
        # This represents the PnL of the trade after time T.
        # If Markout is negative, the taker lost money (market moved against them or they paid spread).
        
        merged_df[f'markout_{interval}s'] = np.where(
            merged_df['side'] == 1,
            merged_df[f'mid_price_{interval}s'] - merged_df['exec_price'],
            np.where(
                merged_df['side'] == -1,
                merged_df['exec_price'] - merged_df[f'mid_price_{interval}s'],
                0
            )
        )

    return merged_df

def plot_results(df, ticker):
    """Generate and save plots."""
    
    # Create output directory
    output_dir = os.path.join("slippage", ticker)
    os.makedirs(output_dir, exist_ok=True)
    print(f"Saving plots to {output_dir}...")
    
    # 1. Slippage Histogram
    plt.figure(figsize=(10, 6))
    plt.hist(df['slippage'], bins=50, alpha=0.7, color='blue', edgecolor='black')
    plt.title(f'Execution Slippage Distribution - {ticker}')
    plt.xlabel('Slippage (Cents)')
    plt.ylabel('Frequency')
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(output_dir, 'slippage_dist.png'))
    print("Saved slippage_dist.png")
    
    # 2. Markout Curve (By Trade)
    # Calculate average markout for each interval
    intervals = [1, 10, 30, 60, 300]
    avg_markouts = []
    for interval in intervals:
        col = f'markout_{interval}s'
        avg = df[col].mean()
        avg_markouts.append(avg)
        
    plt.figure(figsize=(10, 6))
    plt.plot(intervals, avg_markouts, marker='o', linestyle='-', color='red')
    plt.title(f'Average Markout Curve (By Trade) - {ticker}')
    plt.xlabel('Time after execution (seconds)')
    plt.ylabel('Average PnL (Cents)')
    plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(output_dir, 'markout_curve_by_trade.png'))
    print("Saved markout_curve_by_trade.png")

    # 3. Markout Curve (By Volume)
    # Calculate volume-weighted average markout for each interval
    avg_markouts_weighted = []
    for interval in intervals:
        col = f'markout_{interval}s'
        # Weighted average: sum(markout * count) / sum(count)
        if df['count'].sum() > 0:
            avg = np.average(df[col], weights=df['count'])
        else:
            avg = 0
        avg_markouts_weighted.append(avg)
        
    plt.figure(figsize=(10, 6))
    plt.plot(intervals, avg_markouts_weighted, marker='o', linestyle='-', color='purple')
    plt.title(f'Average Markout Curve (By Volume) - {ticker}')
    plt.xlabel('Time after execution (seconds)')
    plt.ylabel('Average PnL (Cents)')
    plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    plt.grid(True, alpha=0.3)
    plt.savefig(os.path.join(output_dir, 'markout_curve_by_volume.png'))
    print("Saved markout_curve_by_volume.png")

    # 4. Average Markout Curve by Hour (By Trade)
    # Convert timestamp to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    
    # Group by hour
    df['hour_bucket'] = df['datetime'].dt.floor('h')
    hourly_groups = df.groupby('hour_bucket')
    
    plt.figure(figsize=(12, 8))
    
    # Get unique hours and generate colors
    unique_hours = sorted(df['hour_bucket'].unique())
    colors = plt.cm.viridis(np.linspace(0, 1, len(unique_hours)))
    
    intervals = [1, 10, 30, 60, 300]
    
    for i, hour in enumerate(unique_hours):
        group_df = hourly_groups.get_group(hour)
        # Skip hours with very few trades to avoid noise
        if len(group_df) < 5:
            continue
            
        avg_markouts_hourly = []
        for interval in intervals:
            col = f'markout_{interval}s'
            avg = group_df[col].mean()
            avg_markouts_hourly.append(avg)
            
        label = hour.strftime('%Y-%m-%d %H:00')
        plt.plot(intervals, avg_markouts_hourly, marker='o', linestyle='-', color=colors[i], label=label, alpha=0.7)

    plt.title(f'Average Markout Curve by Hour (By Trade) - {ticker}')
    plt.xlabel('Time after execution (seconds)')
    plt.ylabel('Average PnL (Cents)')
    plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    plt.grid(True, alpha=0.3)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title="Hour")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'markout_curve_hourly_by_trade.png'))
    print("Saved markout_curve_hourly_by_trade.png")

    # 5. Average Markout Curve by Hour (By Volume)
    plt.figure(figsize=(12, 8))
    
    for i, hour in enumerate(unique_hours):
        group_df = hourly_groups.get_group(hour)
        # Skip hours with very few trades to avoid noise
        if len(group_df) < 5:
            continue
            
        avg_markouts_hourly_weighted = []
        for interval in intervals:
            col = f'markout_{interval}s'
            if group_df['count'].sum() > 0:
                avg = np.average(group_df[col], weights=group_df['count'])
            else:
                avg = 0
            avg_markouts_hourly_weighted.append(avg)
            
        label = hour.strftime('%Y-%m-%d %H:00')
        plt.plot(intervals, avg_markouts_hourly_weighted, marker='o', linestyle='-', color=colors[i], label=label, alpha=0.7)

    plt.title(f'Average Markout Curve by Hour (By Volume) - {ticker}')
    plt.xlabel('Time after execution (seconds)')
    plt.ylabel('Average PnL (Cents)')
    plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    plt.grid(True, alpha=0.3)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title="Hour")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'markout_curve_hourly_by_volume.png'))
    print("Saved markout_curve_hourly_by_volume.png")

    # 3. Slippage Paid by Hour
    # datetime already created above
    
    # Group by hour and sum slippage_paid
    hourly_slippage = df.set_index('datetime').resample('h')['slippage_paid'].sum()
    
    # Convert to dollars
    hourly_slippage_dollars = hourly_slippage / 100.0

    plt.figure(figsize=(12, 6))
    ax = hourly_slippage_dollars.plot(kind='bar', color='green', alpha=0.7, edgecolor='black')
    plt.title(f'Total Slippage Paid by Hour - {ticker}')
    plt.xlabel('Hour')
    plt.ylabel('Total Slippage Paid (Dollars)')
    
    # Format Y-axis to show dollars with commas
    from matplotlib.ticker import FuncFormatter
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, p: f'${x:,.0f}'))

    # Format x-axis labels to be more readable
    # Get current xticks and labels
    locs, labels = plt.xticks()
    # Format them
    new_labels = [item.get_text()[:13].replace('T', ' ') for item in labels]
    plt.xticks(locs, new_labels, rotation=45, ha='right')
    
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'slippage_hourly.png'))
    print("Saved slippage_hourly.png")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze slippage and markouts")
    parser.add_argument("ticker", nargs="?", default="KXNFLGAME-25NOV09BUFMIA-BUF", help="Ticker or wildcard pattern to analyze")
    args = parser.parse_args()
    
    db_path = "kalshi_data.db"
    
    # Find matching tickers
    target_tickers = find_tickers(db_path, args.ticker)
    
    if not target_tickers:
        print(f"No tickers found matching pattern: {args.ticker}")
        exit(1)
        
    print(f"Found {len(target_tickers)} matching tickers: {target_tickers}")
    
    all_results = []
    
    for ticker in target_tickers:
        print(f"\n--- Processing {ticker} ---")
        trades, snapshots = load_data(db_path, ticker)
        
        if trades.empty or snapshots.empty:
            print(f"Skipping {ticker}: No data found.")
            continue
            
        results = process_data(trades, snapshots)
        all_results.append(results)
        
    if not all_results:
        print("No valid data processed.")
    else:
        # Combine all results
        combined_df = pd.concat(all_results, ignore_index=True)
        print(f"\nTotal trades analyzed: {len(combined_df)}")
        
        # Determine output name
        if '*' in args.ticker:
            output_name = args.ticker.replace('*', '_COMBINED')
        else:
            output_name = args.ticker
            
        plot_results(combined_df, output_name)
