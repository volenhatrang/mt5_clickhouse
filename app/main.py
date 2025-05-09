from config_connection import initialize_mt5, initialize_clickhouse
from data_processing import *
import logging
import time
from datetime import datetime, timezone, timedelta
import requests
import json
import pandas as pd

def main():

    mt5 = initialize_mt5()
    client = initialize_clickhouse()

    # Fetch and process MT5 data
    # mt5_data = fetch_and_insert_ohlc_data_mt5(client, mt5, ['EURUSD'], ['M1'], [100])

    # Fetch and process Yahoo data
    yahoo_raw = dbl_download_yah_ohlc_intraday_max("^GSPC", "5m", 60, 0)
    yahoo_to_db(client, yahoo_raw, "intraday_data_yahoo")

if __name__ == "__main__":
    main()
