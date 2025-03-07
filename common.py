class CurrencyOHLCV:
    def __init__(self, symbol : str, record_date : str, opening_price : float, highest_price : float, lowest_price : float, closing_price : float, volume : int):
        self.symbol = symbol
        self.date = record_date
        self.open = opening_price
        self.high = highest_price
        self.low = lowest_price
        self.close = closing_price
        self.volume = volume

KAFKA_TOPIC = 'currencyOCHLV'
KAFKA_SERVER = "localhost:9092"
Currency_Symbols = ["BTC", "ETH"]