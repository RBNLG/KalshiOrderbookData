# Kalshi Orderbook Data Collector

A Python application for collecting real-time orderbook and trade data from Kalshi markets via WebSocket API. The application automatically subscribes to markets, stores data in SQLite, and manages market lifecycle events.

## Features

- 🔌 **Real-time WebSocket Connection**: Connects to Kalshi's WebSocket API for live market data
- 📊 **Orderbook Tracking**: Captures orderbook snapshots and deltas, maintaining in-memory state
- 💰 **Trade Data Collection**: Records all trades with timestamps and pricing information
- 🎯 **Event-Based Market Discovery**: Automatically fetches all markets for given event ticker(s)
- 🏁 **Smart Lifecycle Management**: Automatically unsubscribes from markets when they're determined/closed
- 💾 **SQLite Storage**: Efficient local database storage with indexed queries
- 📈 **Pandas Integration**: Query data as pandas DataFrames for easy analysis
- 🔍 **Database Verification**: Built-in utility to verify and inspect collected data

## Requirements

- Python 3.9+
- Kalshi API credentials (access key and private key file)
- Internet connection for WebSocket API access

## Installation

1. Clone or download this repository

2. Create a virtual environment (recommended):
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Create a `.env` file in the project root with your Kalshi API credentials:
```env
KALSHI_ACCESS_KEY=your_access_key_here
KALSHI_PRIVATE_KEY_PATH=/path/to/your/private_key.pem
```

2. Ensure your private key file is accessible at the specified path.

## Usage

### Basic Usage

Collect data for markets in one or more events by providing event ticker(s):

```bash
python kalshi_database.py KXNFLGAME-25NOV10PHIGB
```

### Multiple Event Tickers

You can provide multiple event tickers:

```bash
python kalshi_database.py KXNFLGAME-25NOV10PHIGB KXNBA-25NOV10PHIGB
```

### Debug Mode

Enable verbose debug output:

```bash
python kalshi_database.py KXNFLGAME-25NOV10PHIGB --debug
```

### Verify Database

Check what data has been collected:

```bash
python verify_database.py
```

This will display:
- Total trades and snapshots stored
- Unique tickers in the database
- Time range of collected data
- Breakdown by ticker
- Sample records

## Data Analysis

The project includes two powerful analysis scripts to visualize market dynamics.

### Slippage & Markout Analysis

Analyze trade execution quality, slippage costs, and price impact (markouts) over time.

```bash
# Analyze a specific market
python analyze_slippage.py KXNFLGAME-25NOV09BUFMIA-BUF

# Analyze multiple markets using a wildcard pattern
python analyze_slippage.py "KXNFLGAME*"
```

**Outputs** (saved in `slippage/<ticker>/`):
- `slippage_dist.png`: Histogram of execution slippage (cost to cross the spread).
- `markout_curve_by_trade.png`: Average PnL of trades after N seconds (unweighted).
- `markout_curve_by_volume.png`: Volume-weighted average PnL of trades after N seconds.
- `markout_curve_hourly_*.png`: Hourly breakdown of markouts.
- `slippage_hourly.png`: Total slippage cost paid by hour.

### Imbalance Analysis

Analyze the net volume flow (YES vs NO contracts) for a market, automatically aggregating data from its linked opposing market (e.g., the other side of a sports game).

```bash
# Analyze a market (automatically finds and includes the linked market)
python analyze_imbalance.py KXNFLGAME-25NOV23CLELV-CLE
```

**Outputs** (saved in `imbalance/<ticker>/`):
- `cumulative_imbalance.png`: Cumulative net volume (YES - NO) over time.
- `net_volume_flow.png`: Net volume flow in 5-minute intervals.

## How It Works

1. **Market Discovery**: The application uses the REST API to fetch all market tickers for the provided event ticker(s)

2. **WebSocket Subscription**: Subscribes to three channels for each market:
   - `orderbook_delta`: Real-time orderbook updates
   - `trade`: Trade executions
   - `market_lifecycle_v2`: Market status changes

3. **Data Storage**:
   - **Trades**: Stored immediately when received
   - **Orderbook Snapshots**: Created from initial snapshots and after each delta update
   - **In-Memory State**: Maintains current orderbook state for efficient delta processing

4. **Lifecycle Management**: When a market is determined (closed/settled), the application automatically:
   - Unsubscribes from all channels for that market
   - Stops processing messages for that market
   - Closes the connection when all markets are determined

## Database Schema

### Trades Table
- `id`: Primary key
- `timestamp`: Unix timestamp of the trade
- `ticker`: Market ticker symbol
- `trade_data`: JSON data containing trade details (count, prices, etc.)
- `created_at`: Database insertion timestamp

### Orderbook Snapshots Table
- `id`: Primary key
- `timestamp`: Unix timestamp of the snapshot
- `ticker`: Market ticker symbol
- `snapshot_data`: JSON data containing orderbook levels (yes/no sides with price/size pairs)
- `created_at`: Database insertion timestamp

Both tables have indexes on `(ticker, timestamp)` and `timestamp` for efficient querying.

## Querying Data

The `OrderbookDatabase` class provides methods to query data as pandas DataFrames:

```python
from kalshi_database import OrderbookDatabase

db = OrderbookDatabase("kalshi_data.db")

# Get all trades for a specific ticker
trades_df = db.get_trades_df(ticker="KXNCAAMBGAME-25NOV11LIUAFA-AFA")

# Get trades within a time range
from datetime import datetime
start_ts = int(datetime(2025, 11, 8, 0, 0, 0).timestamp())
end_ts = int(datetime(2025, 11, 8, 23, 59, 59).timestamp())
trades_df = db.get_trades_df(ticker="KXNCAAMBGAME-25NOV11LIUAFA-AFA", 
                              start_ts=start_ts, end_ts=end_ts)

# Get orderbook snapshots
snapshots_df = db.get_orderbook_snapshots_df(ticker="KXNCAAMBGAME-25NOV11LIUAFA-AFA")

db.close()
```

## Project Structure

```
KalshiOrderbookData/
├── kalshi_database.py      # Main application (database class and WebSocket client)
├── analyze_slippage.py     # Slippage and markout analysis script
├── analyze_imbalance.py    # Market imbalance analysis script
├── verify_database.py      # Database verification utility
├── requirements.txt        # Python dependencies
├── README.md              # This file
├── .env                   # Environment variables (not in git)
├── kalshi_data.db         # SQLite database (not in git)
└── venv/                  # Virtual environment (not in git)
```

## Dependencies

- `websockets>=11.0.3`: WebSocket client library
- `pandas>=2.0.0`: Data analysis and DataFrame support
- `python-dotenv>=1.0.0`: Environment variable management
- `kalshi`: Kalshi Python SDK (unofficial)

## Notes

- The database file (`kalshi_data.db`) is created automatically on first run
- The application will continue running until all markets are determined or you interrupt it (Ctrl+C)
- Market tickers are automatically converted to uppercase (as required by the API)
- The application handles pagination when fetching markets for events with many markets

## Troubleshooting

**No markets found**: Ensure the event ticker is correct and the event exists on Kalshi

**Connection errors**: Check your internet connection and API credentials

**Database locked**: Make sure no other process is accessing the database file

**Missing credentials**: Verify your `.env` file exists and contains the correct variable names

## License

This project is provided as-is for educational and research purposes.

