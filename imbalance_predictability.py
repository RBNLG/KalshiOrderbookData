import sqlite3
import pandas as pd
import numpy as np
import json
import argparse
import os
import matplotlib.pyplot as plt
from datetime import datetime

def load_snapshots(db_path, ticker):
    """Load orderbook snapshots for a specific ticker."""
    conn = sqlite3.connect(db_path)
    print(f"Loading snapshots for {ticker}...")
    query = "SELECT timestamp, snapshot_data FROM orderbook_snapshots WHERE ticker = ? ORDER BY timestamp"
    df = pd.read_sql_query(query, conn, params=(ticker,))
    conn.close()
    return df

def parse_snapshot(row):
    """
    Parse a single snapshot row to extract L1 prices and sizes.
    Returns: (best_bid_price, best_bid_size, best_ask_price, best_ask_size)
    """
    try:
        data = json.loads(row['snapshot_data'])
        
        # YES side (Bids)
        yes_levels = data.get('yes', [])
        # NO side (Offers implied from NO bids)
        no_levels = data.get('no', [])
        
        # Best Bid: Max price on YES side
        if yes_levels:
            # yes_levels is list of [price, size], sorted by price usually, but let's be safe
            # We want the highest price
            best_yes = max(yes_levels, key=lambda x: x[0])
            best_bid_price = best_yes[0]
            best_bid_size = best_yes[1]
        else:
            best_bid_price = np.nan
            best_bid_size = np.nan
            
        # Best Ask: Derived from Best NO Bid
        # If I buy NO at X, it's like selling YES at 100-X.
        # So Best Ask Price for YES = 100 - Best NO Bid Price
        # And the size is the same.
        if no_levels:
            # We want the highest NO bid, which corresponds to the lowest YES ask
            best_no = max(no_levels, key=lambda x: x[0])
            best_no_price = best_no[0]
            best_no_size = best_no[1]
            
            best_ask_price = 100 - best_no_price
            best_ask_size = best_no_size
        else:
            best_ask_price = np.nan
            best_ask_size = np.nan
            
        return pd.Series([best_bid_price, best_bid_size, best_ask_price, best_ask_size])
    except Exception as e:
        return pd.Series([np.nan, np.nan, np.nan, np.nan])

def calculate_metrics(df):
    """Calculate Mid Price and Imbalance."""
    print("Parsing snapshots...")
    # Apply parsing
    df[['bid_price', 'bid_size', 'ask_price', 'ask_size']] = df.apply(parse_snapshot, axis=1)
    
    # Drop rows with missing L1 data
    df = df.dropna(subset=['bid_price', 'ask_price']).copy()
    
    # Calculate Mid Price
    df['mid_price'] = (df['bid_price'] + df['ask_price']) / 2
    
    # Calculate Imbalance
    # (BidSize - AskSize) / (BidSize + AskSize)
    # Range: [-1, 1]
    # +1 means all volume is on Bid (Bullish?)
    # -1 means all volume is on Ask (Bearish?)
    df['imbalance'] = (df['bid_size'] - df['ask_size']) / (df['bid_size'] + df['ask_size'])
    
    return df

def analyze_predictability(df, ticker, output_dir):
    """Resample and calculate correlations."""
    
    # Convert to datetime index
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df.set_index('datetime').sort_index()
    
    # Handle duplicate timestamps by keeping the last one
    df = df[~df.index.duplicated(keep='last')]
    
    # Resample to 1 second, forward filling the last known state
    # This aligns everything to a regular grid for easy shifting
    print("Resampling to 1s frequency...")
    df_resampled = df[['mid_price', 'imbalance']].resample('1s').ffill()
    
    horizons = [1, 5, 30, 60, 120, 300]
    correlations = {}
    
    print("Calculating returns and correlations...")
    for h in horizons:
        # Future return: Price(t + h) - Price(t)
        df_resampled[f'ret_{h}s'] = df_resampled['mid_price'].shift(-h) - df_resampled['mid_price']
        
        # Correlation
        corr = df_resampled['imbalance'].corr(df_resampled[f'ret_{h}s'])
        correlations[f'{h}s'] = corr
        print(f"  Horizon {h}s: Correlation = {corr:.4f}")
        
        # Scatter Plot
        plt.figure(figsize=(10, 6))
        plt.scatter(df_resampled['imbalance'], df_resampled[f'ret_{h}s'], alpha=0.1, s=1)
        plt.title(f'Orderbook Imbalance vs {h}s Future Return\n{ticker} (Corr: {corr:.4f})')
        plt.xlabel('Imbalance (Bid-Ask)/(Bid+Ask)')
        plt.ylabel(f'Price Change ({h}s)')
        plt.grid(True, alpha=0.3)
        plt.axhline(0, color='black', lw=1)
        plt.axvline(0, color='black', lw=1)
        
        # Add a trend line
        valid_data = df_resampled.dropna(subset=['imbalance', f'ret_{h}s'])
        if len(valid_data) > 0:
            z = np.polyfit(valid_data['imbalance'], valid_data[f'ret_{h}s'], 1)
            p = np.poly1d(z)
            plt.plot(valid_data['imbalance'], p(valid_data['imbalance']), "r--", alpha=0.8)
            
        plt.savefig(os.path.join(output_dir, f'imbalance_scatter_{h}s.png'))
        plt.close()

    # Correlation Bar Chart
    plt.figure(figsize=(10, 6))
    plt.bar(correlations.keys(), correlations.values(), color='skyblue')
    plt.title(f'Imbalance Predictability by Time Horizon\n{ticker}')
    plt.xlabel('Time Horizon')
    plt.ylabel('Pearson Correlation')
    plt.grid(axis='y', alpha=0.3)
    plt.axhline(0, color='black', lw=1)
    plt.savefig(os.path.join(output_dir, 'imbalance_correlations.png'))
    plt.close()
    
    return correlations

def main():
    parser = argparse.ArgumentParser(description="Analyze orderbook imbalance predictability")
    parser.add_argument("ticker", help="Market ticker to analyze")
    parser.add_argument("--db", default="kalshi_data.db", help="Path to database")
    args = parser.parse_args()
    
    # Setup output directory
    output_dir = os.path.join("alpha_research", "ob_imbalance", "results", args.ticker)
    os.makedirs(output_dir, exist_ok=True)
    
    # Load
    df = load_snapshots(args.db, args.ticker)
    if df.empty:
        print(f"No snapshots found for {args.ticker}")
        return
        
    # Process
    df = calculate_metrics(df)
    
    # Analyze
    analyze_predictability(df, args.ticker, output_dir)
    print(f"\nAnalysis complete. Results saved to {output_dir}")

if __name__ == "__main__":
    main()
