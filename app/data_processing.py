import time
import logging
from datetime import datetime, timezone, timedelta
import requests
import json
import pandas as pd
from config_connection import initialize_mt5, initialize_clickhouse

### MT5 ###

def fetch_ohlc_data_mt5(mt5, symbol, timeframe_list, n_bars_list):
    ohlc_data_all = []
    for timeframe in timeframe_list:
        for n_bars in n_bars_list:
            logging.info(f"Fetching {symbol}, Timeframe: {timeframe}, Bars: {n_bars}")
            if not mt5.symbol_select(symbol, True):
                logging.warning(f"Failed to select symbol: {symbol}")
                continue
            rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, n_bars)
            if rates is None:
                logging.warning(f"No data for {symbol}, Timeframe: {timeframe}, Bars: {n_bars}. Error: {mt5.last_error()}")
                continue
            for rate in rates:
                ohlc_data_all.append({
                    "symbol": symbol,
                    "time": datetime.fromtimestamp(rate[0], timezone.utc),
                    "open": rate[1],
                    "high": rate[2],
                    "low": rate[3],
                    "close": rate[4],
                    "volume": rate[5]
                })
    return ohlc_data_all

def insert_mt5_data_to_clickhouse(client, ohlc_data):
    for data in ohlc_data:
        try:
            client.execute(
                '''
                INSERT INTO mt5.mt5_data_history (symbol, time, open, high, low, close, volume)
                VALUES (%(symbol)s, %(time)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
                ''',
                data
            )
            logging.info(f"Inserted OHLC data: {data}")
        except Exception as e:
            logging.error(f"Error inserting OHLC data: {e}")

def fetch_and_insert_ohlc_data_mt5(mt5, symbols, timeframe_list, n_bars_list):
    for symbol in symbols:
        logging.info(f"Processing symbol: {symbol}")
        ohlc_data = fetch_ohlc_data_mt5(mt5, symbol, timeframe_list, n_bars_list)
        if ohlc_data:
            insert_mt5_data_to_clickhouse(ohlc_data)

def fetch_realtime_mt5(client, mt5, symbols):
    while True:
        for symbol in symbols:
            symbol_info = mt5.symbol_info_tick(symbol)
            if symbol_info:
                data = {
                    "symbol": symbol,
                    "time": datetime.fromtimestamp(symbol_info.time, timezone.utc),
                    "bid": symbol_info.bid,
                    "ask": symbol_info.ask,
                    "volume": symbol_info.volume,
                    "real_volume": getattr(symbol_info, 'real_volume', 0)
                }
                logging.info(f"Inserting real-time data: {data}")
                try:
                    client.execute(
                        '''
                        INSERT INTO mt5.mt5_data (symbol, time, bid, ask, volume, real_volume)
                        VALUES (%(symbol)s, %(time)s, %(bid)s, %(ask)s, %(volume)s, %(real_volume)s)
                        ''',
                        data
                    )
                except Exception as e:
                    logging.error(f"Error inserting real-time data: {e}")
        time.sleep(1)


### YAHOO FINANCE ###
def dbl_download_yah_ohlc_intraday_max(pCodesource="^GSPC", pInterval="5m", pNbdays=60,  Hour_adjust=0):
    MyURL = f"https://query1.finance.yahoo.com/v8/finance/chart/{pCodesource}?region=US&lang=en-US&includePrePost=false&interval={pInterval}&range={pNbdays}d&corsDomain=finance.yahoo.com&.tsrc=finance"
    
    headers = {
        "User-Agent": "R (3.6.1) jsonlite/1.6"
    }

    x_combined = pd.DataFrame()
    
    try:
        response = requests.get(MyURL, headers=headers)
        response.raise_for_status()
        dt_json = response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return x_combined
    
    if dt_json:
        meta = dt_json['chart']['result'][0]['meta']
        hour_adjust = meta['gmtoffset'] / 3600
        if hour_adjust:
            Hour_adjust = hour_adjust
        
        rCodesource = meta['symbol']
        print(f"Processing {rCodesource}")
        
        x_timestamp = pd.DataFrame({'timestamp': dt_json['chart']['result'][0]['timestamp']})
        x_timestamp['ID'] = range(1, len(x_timestamp) + 1)
        
        quote = dt_json['chart']['result'][0]['indicators']['quote'][0]
        df_quote = pd.DataFrame(quote)
        df_quote['ID'] = range(1, len(df_quote) + 1)
        
        if len(df_quote) > 0 and len(x_timestamp) > 0:
            df_combined = pd.merge(x_timestamp, df_quote, on='ID', how='inner')
            df_combined = df_combined.rename(columns={'ID': 'id'})
            df_combined['datetime'] = pd.to_datetime(df_combined['timestamp'], unit='s')
            df_combined['date'] = df_combined['datetime'].dt.date
            df_combined = df_combined[['id','date', 'datetime', 'open', 'high', 'low', 'close', 'volume']]

            df_combined['codesource'] = rCodesource
            df_combined['source'] = 'YAH'            
            df_combined['timestamp_loc'] = df_combined['datetime'] + timedelta(hours=Hour_adjust)
            df_combined['timestamp_vn'] = df_combined['datetime'] + timedelta(hours=7)
            df_combined['datetime'] = df_combined['timestamp_loc']
            df_combined['timestamp'] = df_combined['timestamp_loc']
            df_combined['date'] = df_combined['datetime'].dt.date
            df_combined = df_combined.drop(columns=['timestamp_loc'])
            
            if pInterval == "1d":
                df_combined = df_combined.sort_values('timestamp').drop_duplicates(subset=['date'])

    return df_combined

def yahoo_to_db(client, dataframe, table_name='intraday_data_yahoo'):
    data = dataframe.to_dict('records')
    try:
        client.execute(
            f"INSERT INTO {table_name} (id, date, datetime, open, high, low, close, volume, codesource, source) VALUES",
            data
        )
        print(f"Successfully inserted {len(data)} rows into {table_name}")
        logging.info(f"Successfully inserted {len(data)} rows into {table_name}")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        logging.error(f"Error inserting data into {table_name}: {e}")


    
    

