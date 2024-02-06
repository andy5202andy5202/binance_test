import logging
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
import pandas as pd
import time
from datetime import datetime
import talib

API_KEY = 'apply API_KEY'
SECRET_KEY = 'apply SECRET_KEY'
BASE_URL = 'https://fapi.binance.com'

column = [
    'Timestamp',
    'open',
    'high',
    'low',
    'close',
    'volumn',
    'Close_time',
    'Quote_av',
    'Trades',
    'Tb_base_av',
    'Tb_quote_av',
    'Ignore'
]

time_sec = {
    '1m' : 60,
    '5m' : 300,
    '15m' : 900,
    '30m' : 1800,
    '1h' : 3600,
    '2h' : 7200,
    '4h' : 14400,
    '6h' : 21600,
    '8h' : 28800,
    '1d' : 86400
}

def get_price(symbol):
    try:
        price = UMFutures().ticker_price(symbol)['price']
        return price
    except ClientError as Error:
        logging.error("Found error. status:{}, error code: {}, error message: {}"
                      .format(Error.status_code, Error.error_code, Error.error_message))
        return 0
    
def get_kline(symbol, interval, start_time, end_time):
    try:
        res = UMFutures().klines(symbol, interval,
                                 starttime = start_time, endtime = end_time)
        return res
    except ClientError as Error:
        logging.error("Found error. status:{}, error code: {}, error message: {}"
                      .format(Error.status_code, Error.error_code, Error.error_message))

def get_history_kline(sizes):
    finish_time = cal_timestamp(str(datetime.now()))
    start_time = cal_timestamp('2020-01-01 0:0:0.0')
    end_time = start_time + (time_sec.get(sizes) * 500 * 1000)
    a = pd.DataFrame(get_kline('ETHUSDT', sizes, start_time, end_time), columns = column)
    start_time = end_time
    while start_time < finish_time:
        end_time = start_time + (time_sec.get(sizes) * 500 * 1000)
        b = pd.DataFrame(get_kline('ETHUSDT', sizes, start_time, end_time), columns = column)
        a = pd.concat([a, b], ignore_index = True)
        start_time = end_time
    a.drop_duplicates(keep = 'first', inplace = False, ignore_index = True)
    a['Timestamp'] = pd.to_datetime(a['Timestamp'], unit = 'ms')
    a['Close_time'] = pd.to_datetime(a['Close_time'], unit = 'ms')
    return a
        
def cal_timestamp(stamp):
    datetime_obj = datetime.strptime(stamp, '%Y-%m-%d %H:%M:%S.%f')
    start_time = int(time.mktime(datetime_obj.timetuple()) * 1000 + datetime_obj.microsecond / 1000)
    return start_time

def read_data(kline_data, i):
    return (float(kline_data.loc[i, 'price']),   
        float(kline_data.loc[i, 'ub']),      
        float(kline_data.loc[i, 'boll']),            
        float(kline_data.loc[i, 'lb']))


def pre_process(ohlcv):
    kline_data = pd.DataFrame()
    kline_data['data_time'] = ohlcv['Timestamp']
    kline_data['ub'], kline_data['boll'], kline_data['lb'] = \
    talib.BBANDS(ohlcv['close'], timeperiod = 22, nbdevup = 2.0, nbdevdn = 2.0)
    kline_data['buy_sell'] = ''
    kline_data['price'] = ohlcv['close']
    for i in range(1, len(kline_data)):
        price, ub, boll, lb = read_data(kline_data, i)
        if price > (ub - (ub - boll) / 5):
            kline_data.loc[i, 'buy_sell'] = 'SELL'
        elif price < (lb + (boll - lb) / 5):
            kline_data.loc[i, 'buy_sell'] = 'BUY'
    kline_data.to_csv('test.csv')
    return kline_data


           


