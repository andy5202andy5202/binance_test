import pandas as pd
import logging
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
import time
from datetime import datetime
import talib

column = [
    'Timestamp',
    'openPrice',
    'High',
    'Low',
    'closePrice',
    'Volume',
    'closeTime',
    'Turnover',
    'Trades_number',
    'Active_Buy_Vol',
    'Active_Buy_Turnover',
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

def cal_timestamp(stamp):
    datetime_obj = datetime.strptime(stamp, '%Y-%m-%d %H:%M:%S.%f')
    start_time = int(time.mktime(datetime_obj.timetuple()) * 1000 + datetime_obj.microsecond / 1000)
    return start_time

def get_kline(symbol, interval, start_time, end_time):
    try:
        res = UMFutures().klines(symbol, interval,
                                startTime = start_time, endTime = end_time)
        return res
    except ClientError as Error:
        logging.error("Found error. status:{}, error code: {}, error message: {}"
                    .format(Error.status_code, Error.error_code, Error.error_message))

def get_history_kline(sizes, START_TIME, END_TIME):
    finish_time = cal_timestamp(END_TIME)
    # finish_time = cal_timestamp(str(datetime.now()))
    start_time = cal_timestamp(START_TIME)
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
    a['closeTime'] = pd.to_datetime(a['closeTime'], unit = 'ms')
    return a

def return_useful_column(DATA, List):
    kline = pd.DataFrame()
    for col in List:
        if col in DATA.columns:  
            kline[col] = DATA[col]  
    kline['Direction'] = ''
    return kline

def macd_and_rsi(DATA):
    DATA["macd_f"], DATA["macd_s"], DATA["macd_diff"] = talib.MACD(DATA['closePrice'], fastperiod=12, slowperiod=26, signalperiod=9)
    DATA["RSI"] = talib.RSI(DATA['closePrice'], timeperiod = 14)
    return DATA

def openFee(volume):
    return volume * 0.0004

def closeFee(volume):
    return volume * 0.0002

def strategyOfRSIandMACD(DATA):
    for i in range(50, len(DATA)):
        
    
    
API_KEY = 'apply API_KEY'
SECRET_KEY = 'apply SECRET_KEY'
BASE_URL = 'https://fapi.binance.com'

# '2022-11-11 0:0:0.0'

historyKline = pd.DataFrame()
historyKline = get_history_kline('1m', '2024-3-20 0:0:0.0', '2024-3-31 0:0:0.0')
List = ['Timestamp', 'openPrice', 'closePrice']
historyKline = return_useful_column(historyKline, List)
historyKline = macd_and_rsi(historyKline)
historyKline.to_csv('historyKline.csv')


