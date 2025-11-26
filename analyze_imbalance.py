import sqlite3
import os
import pandas as pd
import json
import matplotlib.pyplot as plt
import argparse
import numpy as np
from datetime import datetime

def find_linked_ticker(db_path, base_ticker):
    """
    Find the linked ticker for a given base ticker.
    Assumes the event ID is the prefix before the last hyphen.
    Example: KXNFLGAME-25NOV23CLELV-CLE -> Event: KXNFLGAME-25NOV23CLELV
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Extract event prefix (everything up to the last hyphen)
    if '-' not in base_ticker:
        print(f"Invalid ticker format: {base_ticker}")
        return None
        
    parts = base_ticker.rsplit('-', 1)
    event_prefix = parts[0]
    
    print(f"Looking for linked markets for event: {event_prefix}")
    
    # Find other tickers with the same prefix
    cursor.execute("SELECT DISTINCT ticker FROM trades WHERE ticker LIKE ?", (event_prefix + '%',))
    tickers = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    # Filter out the base ticker
    linked_tickers = [t for t in tickers if t != base_ticker]
    
    if not linked_tickers:
        print("No linked tickers found.")
        return None
    
    if len(linked_tickers) > 1:
        print(f"Warning: Found multiple linked tickers: {linked_tickers}. Using the first one.")
        
    return linked_tickers[0]

def load_trades(db_path, ticker):
    """Load trades for a specific ticker."""
    conn = sqlite3.connect(db_path)
    
    print(f"Loading trades for {ticker}...")
    trades_df = pd.read_sql_query(
        "SELECT * FROM trades WHERE ticker = ? ORDER BY timestamp",
        conn,
        params=(ticker,)
    )
    
    conn.close()
    return trades_df

def process_trades(trades_df, is_inverse=False):
    """
    Process trades to calculate signed volume.
    is_inverse: If True, flips the sign of the volume (for linked markets).
    """
    if trades_df.empty:
        return pd.DataFrame()
        
    def get_trade_details(trade_json):
        try:
            data = json.loads(trade_json)
            count = data.get('count', 0)
            taker_side = data.get('taker_side', '').lower()
            return count, taker_side
        except:
            return 0, ''

    # Extract count and side
    trades_df[['count', 'taker_side']] = trades_df['trade_data'].apply(
        lambda x: pd.Series(get_trade_details(x))
    )
    
    # Calculate signed volume
    # YES buy = +count
    # NO buy = -count
    trades_df['signed_volume'] = np.where(
        trades_df['taker_side'] == 'yes',
        trades_df['count'],
        np.where(
            trades_df['taker_side'] == 'no',
            -trades_df['count'],
            0
        )
    )
    
    # Invert if this is the linked market
    if is_inverse:
        trades_df['signed_volume'] = -trades_df['signed_volume']
        
    return trades_df

def plot_imbalance(df, base_ticker, linked_ticker):
    """Generate and save imbalance charts."""
    
    output_dir = os.path.join("imbalance", base_ticker)
    os.makedirs(output_dir, exist_ok=True)
    print(f"Saving plots to {output_dir}...")
    
    # Convert timestamp to datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    
    # 1. Cumulative Net Volume
    plt.figure(figsize=(12, 6))
    plt.plot(df['datetime'], df['cumulative_volume'], label='Net Volume (YES - NO)', color='blue')
    
    plt.title(f'Cumulative Trading Imbalance\n{base_ticker} (vs {linked_ticker})')
    plt.xlabel('Time')
    plt.ylabel('Net Contracts (YES - NO)')
    plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'cumulative_imbalance.png'))
    print("Saved cumulative_imbalance.png")
    
    # 2. Net Volume Flow (Resampled)
    # Resample to 5-minute intervals
    resampled = df.set_index('datetime').resample('5min')['signed_volume'].sum()
    
    plt.figure(figsize=(12, 6))
    colors = ['green' if v >= 0 else 'red' for v in resampled]
    plt.bar(resampled.index, resampled, width=0.003, color=colors, alpha=0.7) # width is in days roughly
    
    plt.title(f'Net Volume Flow (5-min intervals)\n{base_ticker} (vs {linked_ticker})')
    plt.xlabel('Time')
    plt.ylabel('Net Volume')
    plt.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, 'net_volume_flow.png'))
    print("Saved net_volume_flow.png")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze trading imbalance with linked markets")
    parser.add_argument("ticker", help="Base market ticker (e.g., KXNFLGAME-25NOV23CLELV-CLE)")
    args = parser.parse_args()
    
    db_path = "kalshi_data.db"
    base_ticker = args.ticker
    
    # 1. Find linked ticker
    linked_ticker = find_linked_ticker(db_path, base_ticker)
    
    if not linked_ticker:
        print(f"Could not find a linked ticker for {base_ticker}. Proceeding with single market analysis.")
        linked_ticker = "NONE"
    else:
        print(f"Found linked ticker: {linked_ticker}")
        
    # 2. Load and process trades
    base_trades = load_trades(db_path, base_ticker)
    base_trades = process_trades(base_trades, is_inverse=False)
    base_trades['source'] = base_ticker
    
    combined_df = base_trades
    
    if linked_ticker != "NONE":
        linked_trades = load_trades(db_path, linked_ticker)
        linked_trades = process_trades(linked_trades, is_inverse=True)
        linked_trades['source'] = linked_ticker
        
        # Combine
        combined_df = pd.concat([base_trades, linked_trades], ignore_index=True)
        
    if combined_df.empty:
        print("No trades found.")
        exit(0)
        
    # 3. Sort and Calculate Cumulative
    combined_df = combined_df.sort_values('timestamp')
    combined_df['cumulative_volume'] = combined_df['signed_volume'].cumsum()
    
    print(f"Total trades processed: {len(combined_df)}")
    print(f"Final Net Imbalance: {combined_df['cumulative_volume'].iloc[-1]}")
    
    # 4. Plot
    plot_imbalance(combined_df, base_ticker, linked_ticker)
