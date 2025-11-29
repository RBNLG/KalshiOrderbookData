import sqlite3
import time
from datetime import datetime, timedelta

def main():
    db_path = "kalshi_data.db"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Calculate timestamp for 7 days ago
        seven_days_ago = datetime.now() - timedelta(days=7)
        timestamp_threshold = int(seven_days_ago.timestamp())

        print(f"Searching for markets with activity since: {seven_days_ago.strftime('%Y-%m-%d %H:%M:%S')}")

        query = """
            SELECT ticker, MAX(timestamp) as last_trade_ts
            FROM trades 
            WHERE timestamp >= ?
            GROUP BY ticker
            ORDER BY last_trade_ts DESC
        """

        cursor.execute(query, (timestamp_threshold,))
        results = cursor.fetchall()

        if results:
            print(f"\nFound {len(results)} active markets in the last 7 days, sorted by recency:\n")
            
            current_date = None
            for ticker, ts in results:
                # Format: Friday November 28 2025
                trade_date = datetime.fromtimestamp(ts).strftime('%A %B %d %Y')
                
                if trade_date != current_date:
                    if current_date is not None:
                        print() # Add newline between groups
                    print(f"{trade_date}:")
                    current_date = trade_date
                
                print(f"- {ticker}")
        else:
            print("\nNo active markets found in the last 7 days.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
