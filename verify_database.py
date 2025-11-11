import sqlite3
import json
from datetime import datetime

def verify_database(db_path: str = "kalshi_data.db"):
    """Verify and display statistics about stored data"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 60)
    print("DATABASE VERIFICATION REPORT")
    print("=" * 60)
    
    # Check trades
    cursor.execute("SELECT COUNT(*) FROM trades")
    trade_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM trades")
    unique_tickers_trades = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM trades")
    trade_times = cursor.fetchone()
    
    print(f"\n📊 TRADES:")
    print(f"  Total trades stored: {trade_count}")
    print(f"  Unique tickers: {unique_tickers_trades}")
    
    if trade_count > 0:
        min_ts, max_ts = trade_times
        min_dt = datetime.fromtimestamp(min_ts) if min_ts else None
        max_dt = datetime.fromtimestamp(max_ts) if max_ts else None
        print(f"  First trade: {min_dt} (timestamp: {min_ts})")
        print(f"  Last trade: {max_dt} (timestamp: {max_ts})")
        
        # Show trades by ticker
        cursor.execute("""
            SELECT ticker, COUNT(*) as count 
            FROM trades 
            GROUP BY ticker 
            ORDER BY count DESC
        """)
        print(f"\n  Trades by ticker:")
        for row in cursor.fetchall():
            print(f"    {row[0]}: {row[1]} trades")
        
        # Show sample trades
        cursor.execute("SELECT ticker, timestamp, trade_data FROM trades ORDER BY timestamp LIMIT 3")
        print(f"\n  Sample trades (first 3):")
        for row in cursor.fetchall():
            ticker, ts, trade_data = row
            trade = json.loads(trade_data)
            dt = datetime.fromtimestamp(ts)
            print(f"    [{dt}] {ticker}: {trade.get('count', 'N/A')} @ {trade.get('yes_price_dollars', 'N/A')}/{trade.get('no_price_dollars', 'N/A')}")
    else:
        print("  ⚠️  No trades found in database")
    
    # Check orderbook snapshots
    cursor.execute("SELECT COUNT(*) FROM orderbook_snapshots")
    snapshot_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM orderbook_snapshots")
    unique_tickers_snapshots = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM orderbook_snapshots")
    snapshot_times = cursor.fetchone()
    
    print(f"\n📈 ORDERBOOK SNAPSHOTS:")
    print(f"  Total snapshots stored: {snapshot_count}")
    print(f"  Unique tickers: {unique_tickers_snapshots}")
    
    if snapshot_count > 0:
        min_ts, max_ts = snapshot_times
        min_dt = datetime.fromtimestamp(min_ts) if min_ts else None
        max_dt = datetime.fromtimestamp(max_ts) if max_ts else None
        print(f"  First snapshot: {min_dt} (timestamp: {min_ts})")
        print(f"  Last snapshot: {max_dt} (timestamp: {max_ts})")
        
        # Show snapshots by ticker
        cursor.execute("""
            SELECT ticker, COUNT(*) as count 
            FROM orderbook_snapshots 
            GROUP BY ticker 
            ORDER BY count DESC
        """)
        print(f"\n  Snapshots by ticker:")
        for row in cursor.fetchall():
            print(f"    {row[0]}: {row[1]} snapshots")
        
        # Show sample snapshot
        cursor.execute("SELECT ticker, timestamp, snapshot_data FROM orderbook_snapshots ORDER BY timestamp LIMIT 1")
        row = cursor.fetchone()
        if row:
            ticker, ts, snapshot_data = row
            snapshot = json.loads(snapshot_data)
            dt = datetime.fromtimestamp(ts)
            yes_levels = len(snapshot.get('yes', []))
            no_levels = len(snapshot.get('no', []))
            print(f"\n  Sample snapshot (first):")
            print(f"    [{dt}] {ticker}: {yes_levels} YES levels, {no_levels} NO levels")
    else:
        print("  ⚠️  No orderbook snapshots found in database")
    
    # Overall summary
    print(f"\n✅ SUMMARY:")
    if trade_count > 0 or snapshot_count > 0:
        print(f"  Database is working! Found {trade_count} trades and {snapshot_count} snapshots.")
    else:
        print(f"  ⚠️  Database exists but no data found. Make sure the WebSocket client is running and receiving messages.")
    
    print("=" * 60)
    
    conn.close()

if __name__ == "__main__":
    verify_database()

