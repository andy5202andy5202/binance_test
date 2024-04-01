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
    DATA['RSI'] = talib.RSI(DATA['closePrice'], timeperiod = 3)
    DATA['ATR'] = talib.ATR(DATA['High'], DATA['Low'], DATA['closePrice'], timeperiod = 5)
    DATA['ADX'] = talib.ADX(DATA['High'], DATA['Low'], DATA['closePrice'], timeperiod = 3)
    DATA['EMA_f'] = talib.EMA(DATA['closePrice'], timeperiod = 2)
    DATA['EMA_s'] = talib.EMA(DATA['closePrice'], timeperiod = 4)
    return DATA

def openFee(volume):
    return volume * 0.0004

def closeFee(volume):
    return volume * 0.0002

def strategyOfRSIandMACD(DATA):
    totalFund = 500
    tradeNum = 25
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
                            closeFee(25),
                            9
                            ) 
    for i in range(position.maxNumberOfKlines+2, len(DATA)-1):
        closePrice = DATA.loc[i,['closePrice']]
        MACD_diff = DATA.loc[i,['MACD_diff']]
        RSI = DATA.loc[i-1,['RSI']]
        ATR = DATA.loc[i,['ATR']]
        EMA_f = DATA.loc[i,['EMA_f']]
        EMA_s = DATA.loc[i,['EMA_f']]
        ADX = DATA.loc[i,['ADX']]
        
        if position.direction == '':
            if (MACD_diff > 0).any() and (DATA.loc[i-1,['MACD_diff']] < 0).any() and \
            ( (RSI < 30).any() or (DATA.loc[i-1,['RSI']] < 30).any() or (DATA.loc[i-2,['RSI']] < 30).any()):
                position.direction = 'LONG'
                position.time = DATA.loc[i,['Timestamp']]
                position.entryPrice = closePrice[0]
                position.startFin -= position.openFee
                position.stopLosePrice = position.entryPrice - 3 * ATR
            
            elif (MACD_diff < 0).any() and (DATA.loc[i-1,['MACD_diff']] > 0).any() and \
            ((RSI > 70).any() or (DATA.loc[i-1,['RSI']] > 70).any() or (DATA.loc[i-2,['RSI']] > 70).any()):
                position.direction = 'SHORT'
                position.time = DATA.loc[i,['Timestamp']]
                position.entryPrice = closePrice[0]
                position.startFin -= position.openFee
                position.stopLosePrice = position.entryPrice + 3 * ATR
                
            resultOfMACD_ATR_ADX_EMA._append({'Type': 'Open ',
                                            'Time': position.time,
                                            'Direction': position.direction,
                                            'entryPrice': position.entryPrice,
                                            'finishPrice': '',
                                            'Fee': position.openFee,
                                            'Profit': '',
                                            'returnRate': '',
                                            'positionChange': ''},
                                            ignore_index=True)
        elif position.direction == 'LONG':
            if (EMA_f < EMA_s).any() and \
                (MACD_diff < 0).any() and \
                (RSI < DATA.loc[i-1,['RSI']]).any() and \
                (ADX > 45).any() and \
                closePrice[0] != position.stopLosePrice:
                
                _profit = (closePrice[0] - position.entryPrice)/position.entryPrice \
                    * position.investFund * position.leverage - closePrice[0] * 0.0004
                resultOfMACD_ATR_ADX_EMA._append({'Type': 'Close',
                                            'Time': DATA.loc[i,['Timestamp']],
                                            'Direction': position.direction,
                                            'entryPrice': position.entryPrice,
                                            'finishPrice': DATA.loc[i,['closePrice']],
                                            'Fee': position.closeFee,
                                            'Profit': _profit,
                                            'returnRate': str(100 * _profit / position.investFund) + '%',
                                            'positionChange': str(100 * (_profit + position.startFin) / position.startFin) + '%'},
                                            ignore_index=True)
                
                position.direction = ''
                position.entryPrice = ''
                position.time = ''
                position.stopLosePrice = ''
                
            elif (closePrice[0] <= position.stopLosePrice).any():
                _profit = (closePrice[0] - position.entryPrice)/position.entryPrice \
                    * position.investFund * position.leverage - closePrice[0] * 0.0004
                resultOfMACD_ATR_ADX_EMA._append({'Type': 'Close',
                                            'Time': DATA.loc[i,['Timestamp']],
                                            'Direction': position.direction,
                                            'entryPrice': position.entryPrice,
                                            'finishPrice': DATA.loc[i,['closePrice']],
                                            'Fee': position.closeFee,
                                            'Profit': _profit,
                                            'returnRate': str(100 * _profit / position.investFund) + '%',
                                            'positionChange': str(100 * (_profit + position.startFin) / position.startFin) + '%'},
                                            ignore_index=True)
                
                position.direction = ''
                position.entryPrice = ''
                position.time = ''
                position.stopLosePrice = ''
            
                
        elif position.direction == 'SHORT':
            if (EMA_f > EMA_s).any() and \
                (MACD_diff > 0).any() and \
                (RSI > DATA.loc[i-1,['RSI']]).any() and \
                (ADX > 45).any() and \
                (closePrice != position.stopLosePrice).any():
                
                _profit = -1 * (closePrice - position.entryPrice)/position.entryPrice \
                    * position.investFund * position.leverage - closePrice * 0.0004
                resultOfMACD_ATR_ADX_EMA._append({'Type': 'Close',
                                            'Time': DATA.loc[i,['Timestamp']],
                                            'Direction': position.direction,
                                            'entryPrice': position.entryPrice,
                                            'finishPrice': DATA.loc[i,['closePrice']],
                                            'Fee': position.closeFee,
                                            'Profit': _profit,
                                            'returnRate': str(100 * _profit / position.investFund) + '%',
                                            'positionChange': str(100 * (_profit + position.startFin) / position.startFin) + '%'},
                                            ignore_index=True)
                
                position.direction = ''
                position.entryPrice = ''
                position.time = ''
                position.stopLosePrice = ''
                
            elif (closePrice[0] >= position.stopLosePrice[0]).any():
                _profit = -1 * (closePrice - position.entryPrice)/position.entryPrice \
                    * position.investFund * position.leverage - closePrice * 0.0004
                resultOfMACD_ATR_ADX_EMA._append({'Type': 'Close',
                                            'Time': DATA.loc[i,['Timestamp']],
                                            'Direction': position.direction,
                                            'entryPrice': position.entryPrice,
                                            'finishPrice': DATA.loc[i,['closePrice']],
                                            'Fee': position.closeFee,
                                            'Profit': _profit,
                                            'returnRate': str(100 * _profit / position.investFund) + '%',
                                            'positionChange': str(100 * (_profit + position.startFin) / position.startFin) + '%'},
                                            ignore_index=True)
                
                position.direction = ''
                position.entryPrice = ''
                position.time = ''
                position.stopLosePrice = ''
                
    resultOfMACD_ATR_ADX_EMA.to_csv('resultOfBackTest.csv')



API_KEY = 'apply API_KEY'
SECRET_KEY = 'apply SECRET_KEY'
BASE_URL = 'https://fapi.binance.com'



# '2022-11-11 0:0:0.0'

historyKline = pd.DataFrame()
historyKline = get_history_kline('1m', '2024-3-30 23:59:0.0', '2024-3-31 0:0:0.0')
historyKline.to_csv('historyKlines.csv')
List = ['Timestamp', 'High', 'Low', 'closePrice']
historyKline = return_useful_column(historyKline, List)
print('get klines done')
# historyKline = macd_rsi_atr_adx_ema(historyKline)
# historyKline['closePrice'].astype(float)
# historyKline['MACD_diff'].astype(float)
# historyKline['RSI'].astype(float)
# historyKline['ATR'].astype(float)
# historyKline['EMA_s'].astype(float)
# historyKline['EMA_f'].astype(float)
# historyKline['ADX'].astype(float)
print('calculate indicator done')
# print(historyKline.loc[10,['closePrice']])
s = historyKline.loc[10,['closePrice']].to_dict()['closePrice']
print(s)
print(type(s))
t = historyKline.loc[10, 'closePrice']
print(t)
print(type(t))
if float(t) == float(s):
    print('Yes')
# test(historyKline)


