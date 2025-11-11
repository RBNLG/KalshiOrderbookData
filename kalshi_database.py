import asyncio
import sqlite3
import json
import argparse
import sys
import os
from datetime import datetime
from typing import Dict, List, Set
import pandas as pd
from dotenv import load_dotenv
import kalshi.auth

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
access_key = os.getenv("KALSHI_ACCESS_KEY")
private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")

if not access_key or not private_key_path:
    raise ValueError("KALSHI_ACCESS_KEY and KALSHI_PRIVATE_KEY_PATH must be set in .env file")

kalshi.auth.set_key(access_key, private_key_path)

from kalshi.rest import market
import kalshi.websocket

class OrderbookDatabase:
    def __init__(self, db_path: str = "kalshi_data.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_tables()
        # In-memory orderbook state: {ticker: {side: {price: size}}}
        self.orderbook_state: Dict[str, Dict[str, Dict[int, int]]] = {}
    
    def _init_tables(self):
        cursor = self.conn.cursor()
        
        # Trades table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                ticker TEXT NOT NULL,
                trade_data TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Orderbook snapshots table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                ticker TEXT NOT NULL,
                snapshot_data TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_ticker_ts ON trades(ticker, timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orderbook_ticker_ts ON orderbook_snapshots(ticker, timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_orderbook_ts ON orderbook_snapshots(timestamp)")
        
        self.conn.commit()
    
    def store_trade(self, ticker: str, timestamp: int, trade_data: dict):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO trades (timestamp, ticker, trade_data) VALUES (?, ?, ?)",
            (timestamp, ticker, json.dumps(trade_data))
        )
        self.conn.commit()
    
    def store_orderbook_snapshot(self, ticker: str, timestamp: int, snapshot: dict):
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO orderbook_snapshots (timestamp, ticker, snapshot_data) VALUES (?, ?, ?)",
            (timestamp, ticker, json.dumps(snapshot))
        )
        self.conn.commit()
    
    def initialize_orderbook_from_snapshot(self, ticker: str, snapshot_msg: dict):
        """Initialize orderbook state from a snapshot message"""
        if ticker not in self.orderbook_state:
            self.orderbook_state[ticker] = {"yes": {}, "no": {}}
        
        # Convert snapshot arrays to dict format
        for price, size in snapshot_msg.get("yes", []):
            self.orderbook_state[ticker]["yes"][price] = size
        
        for price, size in snapshot_msg.get("no", []):
            self.orderbook_state[ticker]["no"][price] = size
        
        # Store the snapshot
        # Use current timestamp since snapshot doesn't have one
        timestamp = int(datetime.now().timestamp())
        self.store_orderbook_snapshot(ticker, timestamp, snapshot_msg)
    
    def update_orderbook_state(self, ticker: str, delta_msg: dict):
        """Update in-memory orderbook state from delta, then store snapshot"""
        if ticker not in self.orderbook_state:
            self.orderbook_state[ticker] = {"yes": {}, "no": {}}
        
        price = delta_msg["price"]
        delta = delta_msg["delta"]
        side = delta_msg["side"]
        
        # Update the orderbook
        current_size = self.orderbook_state[ticker][side].get(price, 0)
        new_size = current_size + delta
        
        if new_size <= 0:
            # Remove price level if size goes to zero or negative
            self.orderbook_state[ticker][side].pop(price, None)
        else:
            self.orderbook_state[ticker][side][price] = new_size
        
        # Convert timestamp from ISO string to integer
        ts_str = delta_msg.get("ts", "")
        if ts_str:
            try:
                # Parse ISO format: '2025-11-08T14:36:53.091704Z'
                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                timestamp = int(dt.timestamp())
            except:
                timestamp = int(datetime.now().timestamp())
        else:
            timestamp = int(datetime.now().timestamp())
        
        # Store snapshot after update
        snapshot = {
            "yes": [[p, s] for p, s in sorted(self.orderbook_state[ticker]["yes"].items())],
            "no": [[p, s] for p, s in sorted(self.orderbook_state[ticker]["no"].items())]
        }
        self.store_orderbook_snapshot(ticker, timestamp, snapshot)
    
    def get_trades_df(self, ticker: str = None, start_ts: int = None, end_ts: int = None) -> pd.DataFrame:
        """Query trades as pandas DataFrame"""
        query = "SELECT * FROM trades WHERE 1=1"
        params = []
        
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker)
        if start_ts:
            query += " AND timestamp >= ?"
            params.append(start_ts)
        if end_ts:
            query += " AND timestamp <= ?"
            params.append(end_ts)
        
        query += " ORDER BY timestamp"
        return pd.read_sql_query(query, self.conn, params=params)
    
    def get_orderbook_snapshots_df(self, ticker: str = None, start_ts: int = None, end_ts: int = None) -> pd.DataFrame:
        """Query orderbook snapshots as pandas DataFrame"""
        query = "SELECT * FROM orderbook_snapshots WHERE 1=1"
        params = []
        
        if ticker:
            query += " AND ticker = ?"
            params.append(ticker)
        if start_ts:
            query += " AND timestamp >= ?"
            params.append(start_ts)
        if end_ts:
            query += " AND timestamp <= ?"
            params.append(end_ts)
        
        query += " ORDER BY timestamp"
        return pd.read_sql_query(query, self.conn, params=params)
    
    def close(self):
        self.conn.close()


class MyClient(kalshi.websocket.Client):
    def __init__(self, db: OrderbookDatabase, market_tickers: List[str], debug: bool = True):
        super().__init__()
        self.db = db
        self.debug = debug
        self.trade_count = 0
        self.snapshot_count = 0
        self.delta_count = 0
        self.other_count = 0
        self.market_tickers = market_tickers
        self.determined_markets: Set[str] = set()
        self.active_markets: Set[str] = set(market_tickers)
    
    async def on_open(self):
        print("🔌 Connected to WebSocket. Subscribing to channels...")
        print(f"📊 Subscribing to {len(self.market_tickers)} markets...")
        
        # Subscribe to orderbook_delta for all markets
        await self.subscribe(["orderbook_delta"], self.market_tickers)
        
        # Subscribe to trade for all markets
        await self.subscribe(["trade"], self.market_tickers)
        
        # Subscribe to market_lifecycle_v2 for all markets
        await self.subscribe(["market_lifecycle_v2"], self.market_tickers)
        
        print(f"✅ Subscriptions sent for {len(self.market_tickers)} markets. Waiting for messages...\n")
    
    async def unsubscribe(self, channels: list[str], tickers: list[str] = []):
        """
        Unsubscribe from one or more channels, optionally specifying market tickers.
        
        :param channels: A list of channel names to unsubscribe from.
        :param tickers: An optional list of market ticker strings.
        """
        unsubscribe_message = {
            "id": self.message_id,
            "cmd": "unsubscribe",
            "params": {"channels": channels},
        }
        if tickers:
            unsubscribe_message["params"]["market_tickers"] = tickers
        
        if self.debug:
            print(f"🔌 Unsubscribing from channels={channels}, tickers={tickers}")
        
        await self.ws.send(json.dumps(unsubscribe_message))
        self.message_id += 1
    
    async def on_message(self, message):
        msg_type = message.get("type")
        msg_data = message.get("msg", {})
        
        # Handle market_lifecycle_v2 messages to detect when markets determine
        if msg_type == "market_lifecycle_v2":
            ticker = msg_data.get("market_ticker")
            status = msg_data.get("status")
            
            if ticker and status:
                if status in ["closed", "settled"] and ticker not in self.determined_markets:
                    self.determined_markets.add(ticker)
                    self.active_markets.discard(ticker)
                    print(f"🏁 Market DETERMINED: {ticker} | Status: {status}")
                    print(f"   Active markets remaining: {len(self.active_markets)}")
                    
                    # Unsubscribe from all channels for this market to reduce bandwidth and server load
                    try:
                        await self.unsubscribe(
                            ["trade", "orderbook_delta", "market_lifecycle_v2"],
                            [ticker]
                        )
                        print(f"   ✅ Unsubscribed from {ticker}")
                    except Exception as e:
                        print(f"   ⚠️  Failed to unsubscribe from {ticker}: {e}")
                    
                    # If all markets are determined, we can close the connection
                    if len(self.active_markets) == 0:
                        print("✅ All markets have been determined. Closing connection...")
                        if self.ws:
                            await self.ws.close()
                        return
            else:
                if self.debug:
                    print(f"⚠️  MARKET_LIFECYCLE_V2 message missing ticker or status: ticker={ticker}, status={status}")
        
        # Skip processing messages for determined markets
        ticker = msg_data.get("market_ticker")
        if ticker and ticker in self.determined_markets:
            return  # Skip processing for determined markets
        
        if self.debug:
            # Show all message types for debugging
            if msg_type not in ["orderbook_delta"]:  # Skip deltas to reduce noise
                print(f"📨 [{msg_type}] Received: {message.get('type', 'unknown')}")
        
        if msg_type == "trade":
            timestamp = msg_data.get("ts")
            if ticker and timestamp:
                self.db.store_trade(ticker, timestamp, msg_data)
                self.trade_count += 1
                count = msg_data.get("count", "?")
                yes_price = msg_data.get("yes_price_dollars", "?")
                no_price = msg_data.get("no_price_dollars", "?")
                print(f"✅ TRADE #{self.trade_count} stored: {ticker} | Count: {count} | Price: {yes_price}/{no_price} | TS: {timestamp}")
            else:
                print(f"⚠️  TRADE message missing ticker or timestamp: ticker={ticker}, ts={timestamp}")
                if self.debug:
                    print(f"   Full message: {message}")
        
        elif msg_type == "orderbook_snapshot":
            if ticker:
                self.db.initialize_orderbook_from_snapshot(ticker, msg_data)
                self.snapshot_count += 1
                yes_levels = len(msg_data.get("yes", []))
                no_levels = len(msg_data.get("no", []))
                print(f"📸 SNAPSHOT #{self.snapshot_count} stored: {ticker} | YES: {yes_levels} levels, NO: {no_levels} levels")
            else:
                print(f"⚠️  SNAPSHOT message missing ticker")
                if self.debug:
                    print(f"   Full message: {message}")
        
        elif msg_type == "orderbook_delta":
            if ticker:
                self.db.update_orderbook_state(ticker, msg_data)
                self.delta_count += 1
                if self.delta_count % 10 == 0:  # Print every 10th delta to reduce noise
                    price = msg_data.get("price_dollars", msg_data.get("price", "?"))
                    delta = msg_data.get("delta", "?")
                    side = msg_data.get("side", "?")
                    print(f"📊 DELTA #{self.delta_count}: {ticker} | {side} @ {price} | Δ{delta}")
            else:
                print(f"⚠️  DELTA message missing ticker")
                if self.debug:
                    print(f"   Full message: {message}")
        
        elif msg_type in ["subscribed", "ok"]:
            # These are confirmation messages, just log briefly
            if self.debug:
                print(f"✓ {msg_type.upper()}: {msg_data}")
        
        else:
            self.other_count += 1
            if self.debug:
                print(f"❓ UNKNOWN message type '{msg_type}': {message}")
    
    async def on_close(self, close_status_code, close_msg):
        print(f"\n📊 FINAL STATS:")
        print(f"   Trades stored: {self.trade_count}")
        print(f"   Snapshots stored: {self.snapshot_count}")
        print(f"   Deltas processed: {self.delta_count}")
        print(f"   Other messages: {self.other_count}")
        await super().on_close(close_status_code, close_msg)


def fetch_markets_for_event_tickers(event_tickers: List[str]) -> List[str]:
    """
    Fetch all market tickers for the given event ticker(s) using the REST API.
    
    Args:
        event_tickers: List of event ticker strings
        
    Returns:
        List of market ticker strings
    """
    all_market_tickers = []
    
    for event_ticker in event_tickers:
        print(f"🔍 Fetching markets for event ticker: {event_ticker}")
        try:
            # Fetch markets for this event ticker
            response = market.GetMarkets(event_ticker=event_ticker, limit=1000)
            
            if "markets" in response:
                markets = response["markets"]
                market_tickers = [m.get("ticker") for m in markets if m.get("ticker")]
                all_market_tickers.extend(market_tickers)
                print(f"   Found {len(market_tickers)} markets for {event_ticker}")
            else:
                print(f"   ⚠️  No markets found in response for {event_ticker}")
                print(f"   Response: {response}")
                
            # Handle pagination if needed
            cursor = response.get("cursor")
            while cursor:
                print(f"   Fetching next page...")
                response = market.GetMarkets(event_ticker=event_ticker, limit=1000, cursor=cursor)
                if "markets" in response:
                    markets = response["markets"]
                    market_tickers = [m.get("ticker") for m in markets if m.get("ticker")]
                    all_market_tickers.extend(market_tickers)
                    print(f"   Found {len(market_tickers)} more markets (total: {len(all_market_tickers)})")
                cursor = response.get("cursor")
                
        except Exception as e:
            print(f"   ❌ Error fetching markets for {event_ticker}: {e}")
            continue
    
    return all_market_tickers


# Usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kalshi Orderbook Data Collector - Subscribe to markets by event ticker(s)"
    )
    parser.add_argument(
        "event_tickers",
        nargs="+",
        help="One or more event ticker(s) to fetch markets for (e.g., KXNFLGAME-25NOV10PHIGB)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug output"
    )
    
    args = parser.parse_args()
    
    # Convert event tickers to uppercase (API requires uppercase)
    event_tickers = [ticker.upper() for ticker in args.event_tickers]
    
    print("🚀 Starting Kalshi Orderbook Data Collector")
    print(f"📋 Event ticker(s): {', '.join(event_tickers)}")
    print()
    
    # Fetch markets for the event ticker(s)
    market_tickers = fetch_markets_for_event_tickers(event_tickers)
    
    if not market_tickers:
        print("❌ No markets found for the given event ticker(s). Exiting.")
        sys.exit(1)
    
    print(f"\n✅ Found {len(market_tickers)} total markets")
    print(f"📊 Market tickers: {', '.join(market_tickers[:10])}{'...' if len(market_tickers) > 10 else ''}\n")
    
    db = OrderbookDatabase("kalshi_data.db")
    ws_client = MyClient(db, market_tickers, debug=args.debug)
    
    try:
        asyncio.run(ws_client.connect())
    except KeyboardInterrupt:
        print("\n⚠️  Shutting down...")
    finally:
        db.close()

