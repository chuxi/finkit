import logging
import os
from datetime import date, timezone, timedelta, datetime


DAILY_VOLUME_HEADER = ["instrument", "rank",
                       "part_name_1", "trading_volume", "trading_volume_change",
                       "part_name_2", "bid_volume", "bid_volume_change",
                       "part_name_3", "ask_volume", "ask_volume_change"]

DAILY_STOCK_HEADER = [
    "varname", "region", "warehouse", "stock", "change", "unit", "unit_multiplier"
]


class FutureExchangeCollector(object):

    def __init__(self,
                 file_path: str = "./data",
                 mydate: date = None,
                 overwrite: bool = False, tz: int = 8) -> None:
        self.file_path = file_path
        self.overwrite = overwrite
        self.tz = timezone(timedelta(hours=tz))
        self.mydate = mydate if mydate is not None else datetime.now(self.tz)
        logging.info("init future exchange collector %s", self.__class__.__name__)

    def crawl_daily_stock(self):
        pass

    def crawl_daily_volume(self):
        pass

    def file_exists(self, out_file) -> bool:
        if os.path.exists(out_file) and not self.overwrite:
            logging.info("data file %s exists, return", out_file)
            return True
        return False

    def mydate_is_weekend(self):
        cal = self.mydate.weekday()
        if cal == 5 or cal == 6:
            logging.info("skipping weekends: %s", self.mydate.isocalendar())
            return True
        return False

