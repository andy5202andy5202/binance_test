import logging
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
import pandas as pd
import time
from datetime import datetime
import talib
from apscheduler.schedulers.blocking import BlockingScheduler
import os
import sys

with open('api_key.txt', 'r') as file:
    lines = file.readlines()

api_keys = {}
for line in lines:
    key, value = line.strip().split(' = ')
    api_keys[key] = value.strip('"')

API_KEY = api_keys['API_KEY']
SECRET_KEY = api_keys['SECRET_KEY']
BASE_URL = api_keys['BASE_URL']


um_futures_client = UMFutures(
    key = API_KEY,
    secret = SECRET_KEY,
    base_url = BASE_URL
)

config_logging(logging, logging.DEBUG)

SYMBOL = 'ETHUSDT'
kline_data = pd.DataFrame()
indicator_csv = pd.DataFrame(columns=['date_time', 'k', 'd', 'j', 'price', 'atr', 'direction'])

#return wallet balance
def get_balance(symbol): 
    try:
        wallet = 0
        response = um_futures_client.balance(recvWindow = 6000)
        logging.info(response)
        for i in range(1, len(response)):
            if response[i].get('asset') == symbol:
                wallet = response[i].get('balance')
                break
        return wallet
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}"
            .format(
                error.status_code,
                error.error_code,
                error.error_message
            )
        )
        return 0

#kline
def get_kline(symbol, interval, limit):
    try:
        res = um_futures_client.klines(symbol, interval, limit = limit)
        return res
    except ClientError as Error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}"
            .format(
                Error.status_code,
                Error.error_code,
                Error.error_message
            )
        )
        return 0
    
#get newest price
def get_price(symbol):
    try:
        res = float(um_futures_client.ticker_price(symbol)['price'])
        return res
    except ClientError as Error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}"
            .format(
                Error.status_code,
                Error.error_code,
                Error.error_message
            )
        )
        return 0
    
#trade
def new_order(symbol, side, quantity):
    try:
        response = um_futures_client.new_order(
            symbol = symbol,
            side = side,
            type = "MARKET",
            quantity = quantity
        )
        logging.info(response)
        return response['orderId']
    except ClientError as Error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}"
            .format(
                Error.status_code,
                Error.error_code,
                Error.error_message
            )
        )
        return 0
    
#get the information(price) of order
def get_order(symbol, orderId):
    try:
        response = um_futures_client.get_all_orders(
            symbol = symbol,
            orderId = orderId,
            recvWindow = 2000
        )
        logging.info(response)
        # print(type(response))
        return float(response[0]['avgPrice'])
    
    except ClientError as Error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}"
            .format(
                Error.status_code,
                Error.error_code,
                Error.error_message
            )
        )
        return 0

def read_data(data, i):
    k = float(data.loc[i, 'k'])
    d = float(data.loc[i, 'd'])
    j = float(data.loc[i, 'j'])
    now_atr = float(data.loc[i, 'atr'])
    pre_atr = float(data.loc[i-1, 'atr'])
    new_price = float(data.loc[i, 'price'])
    pre_price = float(data.loc[i-1, 'price'])
    return k, d, j, now_atr, pre_atr, new_price, pre_price

def indicator_cal():
    ohlcv = pd.DataFrame(get_kline(SYMBOL, '1m', 50),
                         columns = [
                             'timestamp',
                             'open',
                             'high',
                             'low',
                             'close',
                             'volumn',
                             'close_time',
                             'quote_av',
                             'trades',
                             'tb_base_av',
                             'tb_quote_av',
                             'ignore'
                         ])
    
    kline_data['date_time'] = ohlcv['timestamp']
    kline_data['date_time'] = pd.to_datetime(kline_data['date_time'], unit = 'ms')

    kline_data['k'], kline_data['d'] = talib.STOCH(
        ohlcv['high'],
        ohlcv['low'],
        ohlcv['close'],
        fastk_period = 9,
        slowk_period = 3,
        slowd_period = 3
    )
    kline_data['j'] = 3 * kline_data['k'] - 2 * kline_data['d']
    kline_data['price'] = ohlcv['close']
    kline_data['atr'] = talib.ATR(
        ohlcv['high'],
        ohlcv['low'],
        ohlcv['close'],
        timeperiod = 14
    )

    k, d, j, now_atr, pre_atr, new_price, pre_price = read_data(kline_data, 48)
    buy_sell = ''
    if ( new_price - pre_price) / pre_price > ( now_atr - pre_atr ) \
        / pre_atr and ( now_atr > pre_atr):
        if ( j < k ) and ( k < d ) and ( j < 40 ):
            buy_sell = 'BUY'
    elif ( pre_price - new_price) / new_price > ( pre_atr - now_atr ) \
        / now_atr and ( now_atr < pre_atr):
        if ( j > k ) and ( k > d ) and ( j > 60 ):
            buy_sell = 'SELL'

    # print(kline_data)
    # print(buy_sell)

    return buy_sell, kline_data

def execute_trade(symbol, direction, now_direction, trade_num, now_price):
    global trade_flag, open_time, open_price, open_fee, dup_time, dup_profit, start_fin, profit

    if direction == 'BUY':
        if now_price - open_price > 12 * dup_profit:
            if now_direction == '' or now_direction == 'SELL':
                orderId = new_order(symbol, 'SELL', trade_num)
                close_price = get_price(symbol, orderId)
                close_fee = close_price / 20 * trade_num * 0.0002
                profit += (close_price - open_price) * trade_num - open_fee - close_fee
                print(profit)
                reset_variables()
        elif now_price - open_price < -10 * (dup_time + 1):
            if dup_time < 2:
                orderId = new_order(symbol, direction, trade_num * 2)
                temp_price = get_price(symbol)
                open_price = (open_price + temp_price * 2) / 3
                open_fee = open_fee + (temp_price / 20 * trade_num * 2 * 0.0002)
                trade_num = trade_num + (trade_num * 2)
                dup_time += 1
        if dup_time >= 2:
            if now_price - open_price > 5 * dup_profit:
                if now_direction == '' or now_direction == 'SELL':
                    orderId = new_order(symbol, 'SELL', trade_num)
                    close_price = get_order(symbol, orderId)
                    close_fee = close_price / 20 * trade_num * 0.0002
                    profit += (close_price - open_price) * trade_num - open_fee - close_fee
                    print(profit)
                    reset_variables()
    elif direction == 'SELL':
        if open_price - now_price > 12 * dup_profit:
            if now_direction == '' or now_direction == 'BUY':
                orderId = new_order(symbol, 'BUY', trade_num)
                close_price = get_price(symbol)
                close_fee = close_price / 20 * trade_num * 0.0002
                profit += (close_price - open_price) * trade_num - open_fee - close_fee
                print(profit)
                reset_variables()
        elif open_price - now_price < -10 * (dup_time + 1):
            if dup_time < 2:
                orderId = new_order(symbol, direction, trade_num * 2)
                temp_price = get_price(symbol)
                open_price = (open_price + temp_price * 2) / 3
                open_fee = open_fee + (temp_price / 20 * trade_num * 2 * 0.0002)
                trade_num = trade_num + (trade_num * 2)
                dup_time += 1
        if dup_time >= 2:
            if open_price - now_price > 50 * dup_profit:
                if now_direction == '' or now_direction == 'BUY':
                    orderId = new_order(symbol, 'BUY', trade_num)
                    close_price = get_order(symbol, orderId)
                    close_fee = (close_price / 20 * trade_num * 0.0002)
                    profit += (close_price - open_price) * trade_num - open_fee - close_fee
                    print(profit)
                    reset_variables()

def reset_variables():
    global trade_flag, open_time, direction, open_price, open_fee, dup_time, dup_profit, start_fin, trade_num
    trade_flag = False
    open_time = ''
    direction = ''
    open_price = 0.0
    start_fin = float(get_balance('USDT'))
    trade_num = int(start_fin / 100) * 0.01
    open_fee = 0.0
    close_fee = 0.0
    dup_time = 0
    dup_profit = 1

# def get_indicator_csv(kline_data, direction):
#     global indicator_csv
#     kline_data['direction'] = direction
#     indicator_csv = indicator_csv._append(kline_data, ignore_index=True)
#     indicator_csv.to_csv('indicator.csv', index=False) 

def get_indicator_csv(kline_data, direction):
    global indicator_csv
    kline_data['direction'] = direction
    
    if indicator_csv.empty:
        indicator_csv = kline_data.copy()
    else:
        indicator_csv = indicator_csv._append(kline_data.iloc[-1], ignore_index=True)
    indicator_csv.to_csv('indicator.csv', index=False) 

def change_leverage(SYMBOL, leverage):
    try:
        response = um_futures_client.change_leverage(
            symbol = SYMBOL, leverage = leverage, recvWindows = 6000
        )
        logging.info(response)
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

#check the position mode
def get_position_mode():
    try:
        response = um_futures_client.get_position_mode(recvWindow=2000)
        logging.info(response)
        if ( response['dualSidePosition'] == False ):
            return 'one direction'
        else:
            return 'dual direction'
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

#chang the position mode, string 'True' to set dual mode
def chang_position_mode(FLAG):
    try:
        response = um_futures_client.change_position_mode(
            dualSidePosition = FLAG, recvWindow=2000
        )
        logging.info(response)
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

# change the margin type, symbol is like "ETHUSDT", type is "ISOLATED" or "CROSSED"
def change_margin_type(SYMBOL, TYPE):
    try:
        response = um_futures_client.change_margin_type(
            symbol = SYMBOL, marginType = TYPE, recvWindow=6000
        )
        logging.info(response)
    except ClientError as error:
        logging.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )

change_margin_type(SYMBOL, "CROSSED")

# def report_csv():


def auto():
    global trade_flag, open_time, direction, open_price, start_fin, trade_num, \
          open_fee, close_fee, dup_time, dup_profit, profit
    if not trade_flag:
        print('time: {}'.format(datetime.now()))
        direction, kline_data = indicator_cal()
        get_indicator_csv(kline_data, direction)
        if direction != '':
            open_time = datetime.now()
            orderId = new_order(SYMBOL, direction, trade_num)
            open_price = get_price(SYMBOL)
            trade_flag = True
            open_fee = open_price / 20 * trade_num * 0.0004
            print('open time: {}, open price: {}, trade num: {}, open fee: {}'
                    .format(open_time, open_price, trade_num, open_fee))
        else:
            print('no trade')
    elif trade_flag:
        now_price = get_price(SYMBOL)
        now_direction, kline_data = indicator_cal()
        get_indicator_csv(kline_data, now_direction)
        execute_trade(SYMBOL, direction, now_direction, trade_num, now_price)
    
# if __name__ == '__main__':
#     trade_flag = False
#     open_time = ''
#     direction = ''
#     open_price = 0.0
#     start_fin = float(get_balance('USDT'))
#     trade_num = int(start_fin / 100) * 0.01
#     open_fee = 0.0
#     close_fee = 0.0
#     dup_time = 0
#     dup_profit = 1
#     profit = 0
#     if os.path.exists('indicator.csv'):
#         os.remove('indicator.csv')
#         print("Old 'indicator.csv' deleted.")
#     kline_data.to_csv('indicator.csv', index=False)
#     print("New 'indicator.csv' created.")

#     scheduler = BlockingScheduler(timezone = "Asia/Shanghai")
#     scheduler.add_job(auto, 'interval', minutes = 1)
#     scheduler.start()

    
