from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import sqlite3
import json
import time
from datetime import datetime
from kalshi_database import OrderbookDatabase
import os

app = Flask(__name__, static_folder='static')
CORS(app)  # Enable CORS for all routes

db = OrderbookDatabase("kalshi_data.db")

@app.route('/')
def index():
    """Serve the main HTML page"""
    return send_from_directory('static', 'index.html')

@app.route('/api/markets')
def get_markets():
    """Get list of unique market tickers that have updates in the last hour"""
    one_hour_ago = int(time.time()) - 3600
    
    cursor = db.conn.cursor()
    
    # Get tickers from trades
    cursor.execute("""
        SELECT DISTINCT ticker 
        FROM trades 
        WHERE timestamp >= ?
    """, (one_hour_ago,))
    trade_tickers = {row[0] for row in cursor.fetchall()}
    
    # Get tickers from snapshots
    cursor.execute("""
        SELECT DISTINCT ticker 
        FROM orderbook_snapshots 
        WHERE timestamp >= ?
    """, (one_hour_ago,))
    snapshot_tickers = {row[0] for row in cursor.fetchall()}
    
    # Combine and sort
    active_tickers = sorted(list(trade_tickers | snapshot_tickers))
    
    return jsonify({"markets": active_tickers})

def parse_timestamp(ts):
    """Parse timestamp from database (could be int, str, or ISO format)"""
    if isinstance(ts, int):
        return ts
    if isinstance(ts, str):
        try:
            # Try parsing as ISO format
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            return int(dt.timestamp())
        except:
            try:
                # Try parsing as integer string
                return int(ts)
            except:
                return int(datetime.now().timestamp())
    return int(datetime.now().timestamp())

@app.route('/api/volume/<ticker>')
def get_volume(ticker):
    """Get contract volume over time, bucketed by minute"""
    cursor = db.conn.cursor()
    
    # Get volume bucketed by minute for the last 24 hours
    twenty_four_hours_ago = int(time.time()) - 86400
    
    cursor.execute("""
        SELECT 
            strftime('%Y-%m-%d %H:%M:00', datetime(timestamp, 'unixepoch')) as minute_bucket,
            SUM(json_extract(trade_data, '$.count')) as volume
        FROM trades 
        WHERE ticker = ? AND timestamp >= ?
        GROUP BY minute_bucket
        ORDER BY minute_bucket
    """, (ticker, twenty_four_hours_ago))
    
    volume_data = []
    for row in cursor.fetchall():
        minute_str, volume = row
        # Convert minute string back to timestamp
        dt = datetime.strptime(minute_str, '%Y-%m-%d %H:%M:%S')
        timestamp = int(dt.timestamp())
        
        volume_data.append({
            'timestamp': timestamp,
            'datetime': minute_str,
            'volume': volume
        })
    
    return jsonify({"ticker": ticker, "volume": volume_data})

@app.route('/api/stats')
def get_stats():
    """Get overall statistics about the database"""
    cursor = db.conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM trades")
    total_trades = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT ticker) FROM trades")
    unique_markets = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM trades")
    min_ts_raw, max_ts_raw = cursor.fetchone()
    
    min_ts = parse_timestamp(min_ts_raw) if min_ts_raw else None
    max_ts = parse_timestamp(max_ts_raw) if max_ts_raw else None
    
    return jsonify({
        "total_trades": total_trades,
        "unique_markets": unique_markets,
        "first_trade": datetime.fromtimestamp(min_ts).isoformat() if min_ts else None,
        "last_trade": datetime.fromtimestamp(max_ts).isoformat() if max_ts else None
    })

if __name__ == '__main__':
    # Create static directory if it doesn't exist
    os.makedirs('static', exist_ok=True)
    
    port = 5001
    print("🚀 Starting Kalshi Orderbook Web Server")
    print(f"📊 Open http://localhost:{port} in your browser")
    app.run(debug=True, host='0.0.0.0', port=port)

