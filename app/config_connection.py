import os
from dotenv import load_dotenv
from mt5linux import MetaTrader5
from clickhouse_driver import Client
import logging
import time
from datetime import datetime, timezone, timedelta
import requests
import json
import pandas as pd

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Environment variables
MT5_HOST = os.getenv("MT5_HOST")
MT5_PORT = os.getenv("MT5_PORT")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))

# Initialize MT5 connection
def initialize_mt5():
    mt5 = MetaTrader5(MT5_HOST, MT5_PORT)
    terminal_path = 'C:\\Program Files\\MetaTrader 5\\terminal64.exe'
    if not mt5.initialize(path=terminal_path):
        logging.error("Failed to initialize MT5: %s", mt5.last_error())
        exit()
    return mt5

# Initialize ClickHouse connection
def initialize_clickhouse():
    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    client.execute('''CREATE DATABASE IF NOT EXISTS mt5;''')
    client.execute('''CREATE TABLE IF NOT EXISTS mt5.mt5_data (
        symbol String,
        time DateTime,
        bid Float32,
        ask Float32,
        volume Int32,
        real_volume Int64
    ) ENGINE = MergeTree()
    ORDER BY (symbol, time)''')

    client.execute('''CREATE TABLE IF NOT EXISTS mt5.mt5_data_history (
        symbol String,
        time DateTime('UTC'),
        open Float32,
        high Float32,
        low Float32,
        close Float32,
        volume Int32
    ) ENGINE = MergeTree()
    ORDER BY (symbol, time)''')

    client.execute('''CREATE TABLE IF NOT EXISTS intraday_data_yahoo (
        id Int64,
        date Date,
        datetime DateTime,
        open Float64,
        high Float64,
        low Float64,
        close Float64,
        volume Float64,
        codesource String,
        source String
    ) ENGINE = MergeTree()
    ORDER BY (datetime, codesource)''')
    
    return client
