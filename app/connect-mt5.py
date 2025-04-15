import os
import time
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from mt5linux import MetaTrader5
from clickhouse_driver import Client

# Load environment variables
load_dotenv()

MT5_HOST = os.getenv("MT5_HOST")
MT5_PORT = os.getenv("MT5_PORT")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
API_URL = "https://api-mt5-signal.wealthfarming.org/mt5/symbols"
API_KEY = os.getenv("API_KEY", "a26913d1-2adb-4d84-af7f-2ca3af3506a8")
API_KEY = 'a26913d1-2adb-4d84-af7f-2ca3af3506a8'
# Initialize MetaTrader 5 connection
mt5 = MetaTrader5(MT5_HOST, MT5_PORT)
terminal_path = 'C:\\Program Files\\MetaTrader 5\\terminal64.exe'
if not mt5.initialize(path=terminal_path):
    print("Failed to initialize MT5:", mt5.last_error())
    exit()

# Initialize ClickHouse client
client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

# Create necessary databases and tables
client.execute('''
CREATE DATABASE IF NOT EXISTS mt5;
''')

client.execute('''
CREATE TABLE IF NOT EXISTS mt5.mt5_data (
    symbol String,
    time DateTime,
    bid Float32,
    ask Float32,
    volume Int32,
    real_volume Int64
) ENGINE = MergeTree()
ORDER BY (symbol, time)
''')

client.execute('''
CREATE TABLE IF NOT EXISTS mt5.mt5_data_history (
    symbol String,
    time DateTime,
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume Int32
) ENGINE = MergeTree()
ORDER BY (symbol, time)
''')

def fetch_symbols_from_api():
    """
    Fetch symbols from external API.
    """
    headers = {"x-api-key": API_KEY}
    try:
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching symbols: {e}")
        return []

def fetch_ohlc_data(symbol, timeframe, n_bars=1000):
    """
    Fetch OHLC data from MetaTrader 5 for a given symbol and timeframe.
    """
    if not mt5.symbol_select(symbol, True):
        print(f"Failed to select symbol: {symbol}")
        return []

    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, n_bars)
    if rates is None:
        print(f"No data fetched for symbol: {symbol}, timeframe: {timeframe}. Error: {mt5.last_error()}")
        return []

    ohlc_data = []
    for rate in rates:
        ohlc_data.append({
            "symbol": symbol,
            "time": datetime.fromtimestamp(rate[0], timezone.utc),
            "open": rate[1],
            "high": rate[2],
            "low": rate[3],
            "close": rate[4],
            "volume": rate[5]
        })

    return ohlc_data

def insert_ohlc_data_to_clickhouse(ohlc_data):
    """
    Insert OHLC data into ClickHouse.
    """
    for data in ohlc_data:
        try:
            client.execute(
                '''
                INSERT INTO mt5.mt5_data_history (symbol, time, open, high, low, close, volume)
                VALUES (%(symbol)s, %(time)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
                ''',
                data
            )
            print(f"Inserting OHLC data: {data}")
        except Exception as e:
            print(f"Error inserting OHLC data: {e}")

def fetch_and_insert_ohlc_data(symbols, timeframe):
    """
    Fetch and insert OHLC data for multiple symbols.
    """
    for symbol in symbols:
        ohlc_data = fetch_ohlc_data(symbol, timeframe)
        if ohlc_data:
            insert_ohlc_data_to_clickhouse(ohlc_data)
        else:
            print(f"No data for symbol: {symbol}")

def fetch_realtime_data():
    """
    Fetch real-time data from MetaTrader 5, including real volume if available, and insert it into ClickHouse.
    """
    symbols = fetch_symbols_from_api()
    if not symbols:
        print("No symbols provided.")
        return

    fetch_and_insert_ohlc_data(symbols, mt5.TIMEFRAME_M1)
    while True:
        for symbol in symbols:
            symbol_info = mt5.symbol_info_tick(symbol)
            if symbol_info:
                real_volume = getattr(symbol_info, 'real_volume', 0)
                data = {
                    "symbol": symbol,
                    "time": datetime.utcfromtimestamp(symbol_info.time),
                    "bid": symbol_info.bid,
                    "ask": symbol_info.ask,
                    "volume": symbol_info.volume,
                    "real_volume": real_volume
                }
                print(f"Inserting real-time data: {data}")
                try:
                    client.execute(
                        '''
                        INSERT INTO mt5.mt5_data (symbol, time, bid, ask, volume, real_volume)
                        VALUES (%(symbol)s, %(time)s, %(bid)s, %(ask)s, %(volume)s, %(real_volume)s)
                        ''',
                        data
                    )
                except Exception as e:
                    print(f"Error inserting real-time data: {e}")
        time.sleep(1)

if __name__ == "__main__":
    try:
        symbols = fetch_symbols_from_api()
        if symbols:
            fetch_and_insert_ohlc_data(symbols, mt5.TIMEFRAME_M5)
        else:
            print("No symbols fetched from API.")
    except KeyboardInterrupt:
        print("Data fetching stopped by user.")
    finally:
        mt5.shutdown()
