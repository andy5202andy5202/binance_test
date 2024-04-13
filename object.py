Symbols = ["ETHUSDT", "SOLUSDT", "DOGEUSDT"]

class Position():
    def __init__(self, 
                symbol, 
                time, 
                DIRECTION, 
                leverage, 
                positionSide,
                openPrice,
                openFee
                ):

        self.symbol = symbol
        self.time = time
        self.direction = DIRECTION
        self.leverage = leverage
        self.positionSide = positionSide
        self.openPrice = openPrice
        self.openFee = openFee
        self.closeFee = 0
        self.dupTime = 1
        self.dupProfit = 0
        self.profit = 0