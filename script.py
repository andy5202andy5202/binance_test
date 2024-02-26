import logging
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
import pandas as pd
import time
from datetime import datetime
import talib



um_futures_client = UMFutures(
    key = API_KEY,
    secret = SECRET_KEY,
    base_url = BASE_URL
)

SYMBOL = 'ETHUSDT'
kline_data = pd.DataFrame()

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

    return buy_sell, kline_data

if __name__ == '__main__':
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
    while True:
        new_time = time.time()
        if int(new_time) % 60 == 0:
            old_time = new_time - 60
            print(old_time, new_time)
            break
    while True:
        if not trade_flag:
            new_time = time.time()
            if new_time - old_time >= 60:
                print('start_time = {}'.format(datetime.now()))
                direction, kline_data = indicator_cal()
                print('end_time = {}'.format(datetime.now()))
                print(datetime.now())
                if direction != '':
                    open_time = datetime.now()
                    orderId = new_order(SYMBOL, direction, trade_num)
                    open_price = get_price(SYMBOL, orderId)
                    trade_flag = True
                    open_fee = open_price / 20 * trade_num * 0.0004
                    print('open time: {}, open price: {}, trade num: {}, open fee: {}'
                          .format(open_time, open_price, trade_num, open_fee))
                old_time = new_time
        elif trade_flag:
            new_time = time.time()
            if new_time - old_time >= 60:
                now_price = get_price(SYMBOL)
                new_time = time.time()
                if new_time - old_time >= 60:
                    now_direction, kline_data = indicator_cal()
                    old_time = new_time
                else:
                    now_direction = direction
                if direction == 'BUY':
                    if now_price - open_price > 120 * dup_profit:
                        if now_direction == '' or now_direction == 'SELL':
                            orderId = new_order(SYMBOL, 'SELL', trade_num)
                            close_price = get_price(SYMBOL, orderId)
                            close_fee = close_price / 20 * trade_num * 0.0002
                            profit = (close_price - open_price) * trade_num - open_fee - close_fee
                            open_fee2 = ''
                            dup_time = 0
                            direction = ''
                            open_time = ''
                            open_price = 0.0
                            dup_profit = 1
                            start_fin = float(get_balance('USDT'))
                            trade_num = int(start_fin / 100) * 0.01
                            trade_flag = False
                        else:
                            dup_profit += 1
                    elif now_price - open_price < -100 * (dup_time + 1):
                        if dup_time < 2:
                            orderId = new_order(SYMBOL, direction, trade_num * 2)
                            temp_price = get_price(SYMBOL, orderId)
                            open_price = (open_price + temp_price * 2) / 3
                            open_fee = open_fee + (temp_price / 20 * trade_num * 2 * 0.0002)
                            trade_num = trade_num + (trade_num * 2)
                            dup_time += 1
                    if dup_time >= 2:
                        if now_price - open_price > 50 * dup_profit:
                            if now_direction == '' or now_direction == 'SELL':
                                orderId = new_order(SYMBOL, trade_num)
                                close_price = get_order(SYMBOL, orderId)
                                close_fee = close_price / 20 * trade_num * 0.0002
                                profit = (close_price - open_price) * trade_num - open_fee - close_fee
                                open_fee2 = ''
                                dup_time = 0
                                direction = ''
                                open_time = ''
                                open_price = 0.0
                                dup_profit = 1
                                start_fin = float(get_balance('USDT'))
                                trade_num = int(start_fin / 100) * 0.01
                                trade_flag = False
                            else:
                                dup_profit += 1
                elif direction == 'SELL':
                    if open_price - now_price > 120 * dup_profit:
                        if now_direction == '' or now_direction == 'BUY':
                            orderId = new_order(SYMBOL, 'BUY', trade_num)
                            close_price = get_price(SYMBOL, orderId)
                            close_fee = close_price / 20 * trade_num * 0.0002
                            profit = (close_price - open_price) * trade_num - open_fee - close_fee
                            open_fee2 = ''
                            dup_time = 0
                            direction = ''
                            open_time = ''
                            open_price = 0.0
                            dup_profit = 1
                            start_fin = float(get_balance('USDT'))
                            trade_num = int(start_fin / 100) * 0.01
                            trade_flag = False
                    elif open_price - now_price < -100 * (dup_time + 1):
                        if dup_time < 2:
                            orderId = new_order(SYMBOL, direction, trade_num * 2)
                            temp_price = get_price(SYMBOL, orderId)
                            open_price = (open_price + temp_price * 2) / 3
                            open_fee = open_fee + (temp_price / 20 * trade_num * 2 * 0.0002)
                            trade_num = trade_num + (trade_num * 2)
                            dup_time += 1
                    if dup_time >= 2:
                        if open_price - now_price > 50 * dup_profit:
                            if now_direction == '' or now_direction == 'BUY':
                                orderId = new_order(SYMBOL, trade_num)
                                close_price = get_order(SYMBOL, orderId)
                                close_fee = (close_price / 20 * trade_num * 0.0002)
                                profit = (close_price - open_price) * trade_num - open_fee - close_fee
                                open_fee2 = ''
                                dup_time = 0
                                direction = ''
                                open_time = ''
                                open_price = 0.0
                                dup_profit = 1
                                start_fin = float(get_balance('USDT'))
                                trade_num = int(start_fin / 100) * 0.01
                                trade_flag = False
                            else:
                                dup_profit += 1
                        


# orderId = new_order('ETHUSDT', 'BUY', 0.01)
# print(orderId)
# price = get_order('ETHUSDT', 1289851979)
# print(price)
# kline_data = pd.DataFrame(get_kline('ETHUSDT', '1m', 50), columns = [
#                              'timestamp',
#                              'open',
#                              'high',
#                              'low',
#                              'close',
#                              'volumn',
#                              'close_time',
#                              'quote_av',
#                              'trades',
#                              'tb_base_av',
#                              'tb_quote_av',
#                              'ignore'
#                          ])
# kline_data.to_csv('test.csv')
# indicator_cal()
# kline_data.to_csv('kline.csv')
