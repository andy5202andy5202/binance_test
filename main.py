import pandas as pd
import header as h
import time
from datetime import datetime
        
if __name__ == '__main__':
    ohlcv = h.get_history_kline('1d')
    ohlcv = h.pre_process(ohlcv)
    # print(ohlcv.columns)
    profit = h.back_test(ohlcv)
    print(profit)