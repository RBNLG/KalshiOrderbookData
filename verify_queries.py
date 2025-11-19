import sqlite3
import json
import time
from datetime import datetime

db_path = "kalshi_data.db"
conn = sqlite3.connect(db_path)

def test_active_markets():
    print("--- Testing Active Markets ---")
    one_hour_ago = int(time.time()) - 3600
    
    # Check trades
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT ticker 
        FROM trades 
        WHERE timestamp >= ?
    """, (one_hour_ago,))
    trade_tickers = [row[0] for row in cursor.fetchall()]
    print(f"Active markets (trades): {len(trade_tickers)}")
    
    # Check orderbook snapshots
    cursor.execute("""
        SELECT DISTINCT ticker 
        FROM orderbook_snapshots 
        WHERE timestamp >= ?
    """, (one_hour_ago,))
    snapshot_tickers = [row[0] for row in cursor.fetchall()]
    print(f"Active markets (snapshots): {len(snapshot_tickers)}")
    
    all_active = set(trade_tickers) | set(snapshot_tickers)
    print(f"Total active markets: {len(all_active)}")
    return list(all_active)

def test_volume_query(ticker):
    print(f"\n--- Testing Volume Query for {ticker} ---")
    # Bucket by minute
    query = """
        SELECT 
            strftime('%Y-%m-%d %H:%M:00', datetime(timestamp, 'unixepoch')) as minute_bucket,
            SUM(json_extract(trade_data, '$.count')) as volume
        FROM trades 
        WHERE ticker = ?
        GROUP BY minute_bucket
        ORDER BY minute_bucket DESC
        LIMIT 10
    """
    cursor = conn.cursor()
    cursor.execute(query, (ticker,))
    rows = cursor.fetchall()
    for row in rows:
        print(row)

active_markets = test_active_markets()
if active_markets:
    test_volume_query(active_markets[0])
else:
    print("No active markets found.")

conn.close()
