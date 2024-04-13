Symbols = ["ETHUSDT", "SOLUSDT", "DOGEUSDT"]

class Position():
        def __init__(self, 
                symbol, 
                time, 
                DIRECTION, 
                leverage,
                starFin,
                investFund, 
                positionSide,
                entryPrice,
                stopLosePrice,
                openFee,
                closeFee,
                maxNumberOfKlines):

                self.symbol = symbol
                self.time = time
                self.direction = DIRECTION
                self.leverage = leverage
                self.startFin = starFin
                self.investFund = investFund
                self.positionSide = positionSide
                self.entryPrice = entryPrice
                self.stopLosePrice = stopLosePrice
                self.openFee = openFee
                self.closeFee = closeFee
                # self.dupTime = 1
                # self.dupProfit = 0
                # self.profit = 0
                self.maxNumberOfKlines = maxNumberOfKlines
        
resultColumn = ['Type',
                'Time',
                'Direction',
                'entryPrice',
                'finishPrice',
                'Fee',
                'Profit',
                'returnRate',
                'positionChange'
                ]