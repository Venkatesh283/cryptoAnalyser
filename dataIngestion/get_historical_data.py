import datetime
import threading
import cryptocompare

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
common_utils = parent_dir + '/' + 'utilities/'
common_utils_dir = os.path.dirname(common_utils)

sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(1, common_utils_dir)

from settings import *
from utils import CommonUtilities
from mongo_utils import MongoUtilities

class GetHistoricalData():
    def __init__(self):
        cryptocompare.cryptocompare._set_api_key_parameter(CRYPTOCOMPARE_KEY)
        self.utils_obj = CommonUtilities()
        self.log = self.utils_obj.init_logger(LOG_FILE)
        self.mongo_obj = MongoUtilities(self.log, MONGO_CREDENTIALS)

    def _get_symbols(self):
        cur = self.mongo_obj.search({}, DATA_DB, DATA_COLLECTION, {'symbol': 1, 'currency': 1, '_id': 0})
        items = {(obj['symbol'], obj['currency']) for obj in cur}

        return list(items)

    def _prepare_records(self, symbol, currency, history):
        historical_records = []
        for item in history:
            average = (item['close'] + item['open']) / 2
            change = item['close'] - item['open']
            percent = (change / item['open']) * 100 if item['open'] else 0

            new_rec = {
                "symbol" : symbol,
                "currency": currency,
                "info" : {
                    "highest_buy_bid" : 0,
                    "lowest_sell_bid" : 0,
                    "last_traded_price" : item['close'],
                    "yes_price" : item['open'],
                    "volume" : {
                        "max" : item["high"],
                        "min" : item["low"],
                        "volume" : item['volumefrom']
                    },
                },
                "timestamp" : datetime.datetime.utcnow().timestamp(),
                "dt_added" : datetime.datetime.utcnow().timestamp(),
                "datetime" : item['time'],
                "high" : item["high"],
                "low": item["low"],
                "bid" : 0,
                "bidVolume" : "",
                "ask" : 0,
                "askVolume" : "",
                "vwap" : "",
                "open" : item['open'],
                "close" : item['close'],
                "last" : item['close'],
                "baseVolume" : item['volumefrom'],
                "quoteVolume" : "",
                "previousClose" : 0,
                "change" : change,
                "percentage" : percent,
                "average" : average
            }

            historical_records.append(new_rec)

        return historical_records

    def get_historical_info(self, symbol, currency='', num_days=1):
        history = cryptocompare.get_historical_price_day(symbol, currency=currency, limit=num_days)
        return history

    def historical_info_to_db(self, input_arr, num_days=1):
        for inp in input_arr:
            symbol, currency = inp
            history = self.get_historical_info(symbol, currency, num_days)
            if not history:
                continue

            historical_records = self._prepare_records(symbol, currency, history)
            self.mongo_obj.upload_to_mongo(historical_records, DATA_DB, DATA_COLLECTION)

        return

    def get_and_upload_historical_info(self, num_days=1):
        symbols = self._get_symbols()

        chunk_size = len(symbols) // MAX_THREADS
        chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

        thrds = []
        for ch in chunks:
            th = threading.Thread(target=self.historical_info_to_db, args=(ch, num_days))
            th.start()

            thrds.append(th)

        for t in thrds:
            t.join()

if __name__ == '__main__':
    GetHistoricalData().get_and_upload_historical_info(NUM_DAYS)
