import pandas as pd
import logging
from binance.um_futures import UMFutures
from binance.lib.utils import config_logging
from binance.error import ClientError
import time
from datetime import datetime
import talib
import data

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
    kline['profit'] = ''
    return kline

def macd_rsi_atr_adx_ema(DATA):
    DATA['MACD_f'], DATA['MACD_s'], DATA['MACD_diff'] = talib.MACD(DATA['closePrice'], fastperiod=3, slowperiod=9, signalperiod=3)
    DATA['RSI'] = talib.RSI(DATA['closePrice'], timeperiod = 5)
    DATA['ATR'] = talib.ATR(DATA['High'], DATA['Low'], DATA['closePrice'], timeperiod = 5)
    DATA['ADX'] = talib.ADX(DATA['High'], DATA['Low'], DATA['closePrice'], timeperiod = 5)
    DATA['EMA_f'] = talib.EMA(DATA['closePrice'], timeperiod = 3)
    DATA['EMA_s'] = talib.EMA(DATA['closePrice'], timeperiod = 6)
    DATA['EMA_f1'] = talib.EMA(DATA['closePrice'], timeperiod = 144)
    DATA['EMA_f2'] = talib.EMA(DATA['closePrice'], timeperiod = 169)
    DATA['EMA_s1'] = talib.EMA(DATA['closePrice'], timeperiod = 288)
    DATA['EMA_s2'] = talib.EMA(DATA['closePrice'], timeperiod = 338)
    DATA['EMA_12'] = talib.EMA(DATA['closePrice'], timeperiod = 12)
    DATA['EMA_26'] = talib.EMA(DATA['closePrice'], timeperiod = 26)
    DATA['KDJ_k'],DATA['KDJ_d'] = talib.STOCH(DATA['High'],\
                                    DATA['Low'],\
                                    DATA['closePrice'],\
                                    fastk_period=2,\
                                    slowk_period=4,\
                                    slowk_matype=0,\
                                    slowd_period=3,\
                                    slowd_matype=0)
    DATA['KDJ_j'] = list(map(lambda x , y: 3 * x - 2 * y, DATA['KDJ_k'], DATA['KDJ_d']))
    DATA['Upper_DON'] = DATA['High'].rolling(window=3).max()
    DATA['Lower_DON'] = DATA['Low'].rolling(window=3).min()
    DATA['Mid_DON'] = (DATA['Upper_DON'] + DATA['Lower_DON']) / 2
    DATA['EMA_DON'] = talib.EMA(DATA['closePrice'], timeperiod = 2)
    return DATA


def openFee(volume):
    return volume * 0.0004

def _closeFee(volume, closePrice, entryPrice):
    return volume * (closePrice/entryPrice) * 0.0004

def _returnRate(volume, direction, closePrice, entryPrice, leverage):
    if direction == 'LONG':
        return (closePrice/entryPrice-1) * leverage - _closeFee(volume, closePrice, entryPrice) / volume
    else:
        return (1-closePrice/entryPrice) * leverage - _closeFee(volume, closePrice, entryPrice) / volume 

def calProfit(direction, closePrice, entryPrice, leverage, volume):
    return _returnRate(volume, direction, closePrice, entryPrice, leverage) * volume 

def strategyOfRSIandMACD(DATA):
    totalFund = 500
    tradeNum = 25

def log_open_position(position):
    new_row = pd.DataFrame({
        'Type': 'open',
        'Time': position.time,
        'Direction': position.direction,
        'entryPrice': position.entryPrice,
        'finishPrice': '',
        'Fee': position.openFee,
        'Profit': '',
        'returnRate': '',
        'positionChange': ''
    }, index=[0])
    return new_row

def log_close_position(position, finishPrice, profit, returnRate):
    new_row = pd.DataFrame({
        'Type': 'Close',
        'Time': position.time,
        'Direction': position.direction,
        'entryPrice': position.entryPrice,
        'finishPrice': finishPrice,
        'Fee': position.closeFee,
        'Profit': profit,
        'returnRate': returnRate,
        'positionChange': (position.startFin+profit)/500
    }, index=[0])
    return new_row


# resultColumn = ['Type',
#                 'Time',
#                 'Direction',
#                 'entryPrice',
#                 'finishPrice',
#                 'openFee',
#                 'closeFee',
#                 'Profit',
#                 'returnRate',
#                 'positionChange']
def test(DATA):
    lose = 0
    win = 0
    resultOfMACD_ATR_ADX_EMA = pd.DataFrame(columns = data.resultColumn)
    position = data.Position('ETH',
                            '',
                            '',
                            20,
                            500,
                            25,
                            False,
                            '',
                            '',
                            openFee(25),
                            '',
                            26
                            ) 
    for i in range(position.maxNumberOfKlines+2, len(DATA)-1):
        openPrice = float(DATA.loc[i,'openPrice'])
        closePrice = float(DATA.loc[i,'closePrice'])
        closePrice_pre = float(DATA.loc[i-1,'closePrice'])
        closePrice_pre_pre = float(DATA.loc[i-2,'closePrice'])
        high = float(DATA.loc[i,'High'])
        high_pre = float(DATA.loc[i-1,'High'])
        high_pre_pre = float(DATA.loc[i-2,'High'])
        low = float(DATA.loc[i,'Low'])
        low_pre = float(DATA.loc[i-1,'Low'])
        low_pre_pre = float(DATA.loc[i-2,'Low'])
        MACD_f = float(DATA.loc[i,'MACD_f'])
        MACD_f_pre = float(DATA.loc[i-1,'MACD_f'])
        MACD_s = float(DATA.loc[i,'MACD_s'])
        MACD_s_pre = float(DATA.loc[i-1,'MACD_s'])
        MACD_diff = float(DATA.loc[i,'MACD_diff'])
        MACD_diff_pre = float(DATA.loc[i-1,'MACD_diff'])
        RSI = float(DATA.loc[i,'RSI'])
        RSI_pre =float(DATA.loc[i-1,'RSI'])
        RSI_pre_pre = float(DATA.loc[i-2, 'RSI'])
        RSI_pre_pre_pre = float(DATA.loc[i-3, 'RSI'])
        ATR = float(DATA.loc[i,'ATR'])
        ATR_pre = float(DATA.loc[i-1, 'ATR'])
        EMA_f = float(DATA.loc[i,'EMA_f'])
        EMA_f_pre = float(DATA.loc[i-1,'EMA_f'])
        EMA_f_pre_pre = float(DATA.loc[i-2,'EMA_f'])
        EMA_f_pre_pre_pre = float(DATA.loc[i-3,'EMA_f'])
        EMA_s = float(DATA.loc[i,'EMA_s'])
        EMA_s_pre = float(DATA.loc[i-1,'EMA_s'])
        EMA_s_pre_pre = float(DATA.loc[i-2,'EMA_s'])
        EMA_s_pre_pre_pre = float(DATA.loc[i-3,'EMA_s'])
        EMA_f1 = float(DATA.loc[i,'EMA_f1'])
        EMA_f2 = float(DATA.loc[i,'EMA_f2'])
        EMA_s1 = float(DATA.loc[i,'EMA_s1'])
        EMA_s2 = float(DATA.loc[i,'EMA_s2'])
        EMA_12 = float(DATA.loc[i,'EMA_12'])
        EMA_12_pre = float(DATA.loc[i-1,'EMA_12'])
        EMA_26 = float(DATA.loc[i,'EMA_26'])
        EMA_26_pre = float(DATA.loc[i-1,'EMA_26'])
        ADX = float(DATA.loc[i,'ADX'])
        ADX_pre = float(DATA.loc[i-1,'ADX'])
        ADX_pre_pre = float(DATA.loc[i-2,'ADX'])
        KDJ_k = float(DATA.loc[i,'KDJ_k'])
        KDJ_k_pre = float(DATA.loc[i-1,'KDJ_k'])
        KDJ_k_pre_pre = float(DATA.loc[i-2,'KDJ_k'])
        KDJ_d = float(DATA.loc[i,'KDJ_d'])
        KDJ_d_pre = float(DATA.loc[i-1,'KDJ_d'])
        KDJ_j = float(DATA.loc[i,'KDJ_j'])
        KDJ_j_pre = float(DATA.loc[i-1,'KDJ_j'])
        Upper_DON = float(DATA.loc[i,'Upper_DON'])
        Upper_DON_pre = float(DATA.loc[i-1,'Upper_DON'])
        Upper_DON_pre_pre = float(DATA.loc[i-2,'Upper_DON'])
        Upper_DON_pre_pre_pre = float(DATA.loc[i-3,'Upper_DON'])
        Lower_DON = float(DATA.loc[i,'Lower_DON'])
        Lower_DON_pre = float(DATA.loc[i-1,'Lower_DON'])
        Lower_DON_pre_pre = float(DATA.loc[i-2,'Lower_DON'])
        Lower_DON_pre_pre_pre = float(DATA.loc[i-3,'Lower_DON'])
        Mid_DON = float(DATA.loc[i,'Mid_DON'])
        Mid_DON_pre = float(DATA.loc[i-1,'Mid_DON'])
        Mid_DON_pre_pre = float(DATA.loc[i-2,'Mid_DON'])
        Mid_DON_pre_pre_pre = float(DATA.loc[i-3,'Mid_DON'])
        EMA_DON = float(DATA.loc[i,'EMA_DON'])
        EMA_DON_pre = float(DATA.loc[i-1,'EMA_DON'])
        EMA_DON_pre_pre = float(DATA.loc[i-2,'EMA_DON'])

        
        if position.direction == '': #做多條件
            if EMA_12 >= EMA_26 and \
                MACD_diff > 0 and \
                (abs(closePrice - low_pre)/low_pre)  > 0.0012 and \
                ADX > ADX_pre and \
                ADX > 60:

                entryPrice_low_pre = low_pre
                entryPrice_high_pre = high_pre
                entryPrice_pre = closePrice_pre                
                entryPrice_Lower_DON_pre = Lower_DON_pre
                entryPrice_Lower_DON = Lower_DON
                position.direction = 'LONG'
                position.time = DATA.loc[i,'Timestamp']
                position.entryPrice = closePrice
                position.startFin -= position.openFee
                position.stopLosePrice = position.entryPrice - 3*ATR
                
                new_row = log_open_position(position)
                resultOfMACD_ATR_ADX_EMA = pd.concat([resultOfMACD_ATR_ADX_EMA, new_row])
                
                continue
            
            elif EMA_12 <= EMA_26 and \
                MACD_diff < 0 and \
                (abs(closePrice - high_pre)/high_pre)   > 0.0012 and \
                ADX > ADX_pre and \
                ADX > 60:#做空條件

                entryPrice_low_pre = low_pre
                entryPrice_high_pre = high_pre
                entryPrice_pre = closePrice_pre
                entryPrice_Upper_DON_pre = Upper_DON_pre
                entryPrice_Upper_DON = Upper_DON
                position.direction = 'SHORT'
                position.time = DATA.loc[i,'Timestamp']
                position.entryPrice = closePrice
                position.startFin -= position.openFee
                position.stopLosePrice = position.entryPrice + 3*ATR
                
                new_row = log_open_position(position)
                resultOfMACD_ATR_ADX_EMA = pd.concat([resultOfMACD_ATR_ADX_EMA, new_row])
                
        elif position.direction == 'LONG':
            if closePrice >= position.entryPrice + abs(position.entryPrice - entryPrice_low_pre) * 1 : #做多止盈
                profit = calProfit(position.direction, closePrice, position.entryPrice, 20, 25)
                position.closeFee = _closeFee(25, closePrice, position.entryPrice)
                returnRate = _returnRate(25,position.direction,closePrice, position.entryPrice, 20)
                new_row = log_close_position(position, DATA.loc[i, 'closePrice'], profit, returnRate)
                resultOfMACD_ATR_ADX_EMA = pd.concat([resultOfMACD_ATR_ADX_EMA, new_row])
 
                position.startFin += profit
                position.direction = ''
                position.entryPrice = ''
                position.time = ''
                position.stopLosePrice = ''
                if profit > 0:
                    win += 1
                else:
                    lose += 1
                
            elif closePrice <= entryPrice_low_pre : #做多止損條件
                profit = calProfit(position.direction, closePrice, position.entryPrice, 20, 25)
                position.closeFee = _closeFee(25, closePrice, position.entryPrice)
                returnRate = _returnRate(25,position.direction,closePrice, position.entryPrice, 20)
                new_row = log_close_position(position, DATA.loc[i, 'closePrice'], profit, returnRate)
                resultOfMACD_ATR_ADX_EMA = pd.concat([resultOfMACD_ATR_ADX_EMA, new_row])

                position.startFin += profit
                position.direction = ''
                position.entryPrice = ''
                position.time = ''
                position.stopLosePrice = ''
                if profit > 0:
                    win += 1
                else:
                    lose += 1
            
                
        elif position.direction == 'SHORT':
            if closePrice <= position.entryPrice - abs(entryPrice_high_pre - position.entryPrice ) * 1 : #做空止盈
                
                profit = calProfit(position.direction, closePrice, position.entryPrice, 20, 25)
                position.closeFee = _closeFee(25, closePrice, position.entryPrice)
                returnRate = _returnRate(25,position.direction,closePrice, position.entryPrice, 20)
                new_row = log_close_position(position, DATA.loc[i, 'closePrice'], profit, returnRate)
                resultOfMACD_ATR_ADX_EMA = pd.concat([resultOfMACD_ATR_ADX_EMA, new_row])

                position.startFin += profit
                position.direction = ''
                position.entryPrice = ''
                position.time = ''
                position.stopLosePrice = ''
                if profit > 0:
                    win += 1
                else:
                    lose += 1
                
            elif closePrice >= entryPrice_high_pre : #做空止損
                profit = calProfit(position.direction, closePrice, position.entryPrice, 20, 25)
                position.closeFee = _closeFee(25, closePrice, position.entryPrice)
                returnRate = _returnRate(25,position.direction,closePrice, position.entryPrice, 20)
                new_row = log_close_position(position, DATA.loc[i, 'closePrice'], profit, returnRate)
                resultOfMACD_ATR_ADX_EMA = pd.concat([resultOfMACD_ATR_ADX_EMA, new_row])
                
                position.startFin += profit
                position.direction = ''
                position.entryPrice = ''
                position.time = ''
                position.stopLosePrice = ''
                if profit > 0:
                    win += 1
                else:
                    lose += 1
                
    resultOfMACD_ATR_ADX_EMA.to_csv('resultOfBackTest.csv')
    print(position.startFin)
    print(win, ' ', lose)



API_KEY = 'apply API_KEY'
SECRET_KEY = 'apply SECRET_KEY'
BASE_URL = 'https://fapi.binance.com'



# '2022-11-11 0:0:0.0'

historyKline = pd.DataFrame()
historyKline = get_history_kline('1m', '2024-01-01 0:0:0.0', '2024-05-31 0:0:0.0') #時間設定
historyKline.to_csv('historyKlines.csv')
print('get klines done')
List = ['Timestamp','openPrice', 'High', 'Low', 'closePrice']
historyKline = return_useful_column(historyKline, List)


historyKline = macd_rsi_atr_adx_ema(historyKline)
historyKline.to_csv('use.csv')
print('calculate indicator done')

test(historyKline)



