import logging
import os
from datetime import date
from urllib import request

from .future_exchange_collector import FutureExchangeCollector, DAILY_VOLUME_HEADER
from .. import utils

CFFEX_DOWNLOAD_URL = "http://www.cffex.com.cn/sj/ccpm/{}/{}_1.csv"
CFFEX_DAILY_VOLUME_FILE_FORMAT = "daily-volume-cffex-{}.csv"

logger = logging.getLogger(__name__)

FUTURE_SYMBOLS = ["IF", "IC", "IM", "IH", "TS", "TF", "T"]


class CffexCollector(FutureExchangeCollector):

    def __init__(self, file_path: str = "./data",
                 mydate: date = None, overwrite: bool = False) -> None:
        super().__init__(file_path, mydate, overwrite)

    def crawl_daily_volume(self):
        out_file = self.file_path + "/" + CFFEX_DAILY_VOLUME_FILE_FORMAT.format(self.mydate.isoformat())
        if self.file_exists(out_file) and self.mydate_is_weekend():
            return
        try:
            volumes = []
            date_string = self.mydate.strftime("%Y%m%d")
            for symbol in FUTURE_SYMBOLS:
                url = CFFEX_DOWNLOAD_URL.format(self.mydate.strftime("%Y%m/%d"), symbol)
                f = request.urlopen(url)
                for line in f.read().decode("gb18030").splitlines():
                    if not line.startswith(date_string):
                        continue
                    parts = line.split(",")
                    if len(parts) == 12:
                        volumes.append({
                            "instrument": parts[1].strip(),
                            "rank": parts[2],
                            "part_name_1": parts[3].strip(),
                            "trading_volume": parts[4],
                            "trading_volume_change": parts[5],
                            "part_name_2": parts[6].strip(),
                            "bid_volume": parts[7],
                            "bid_volume_change": parts[8],
                            "part_name_3": parts[9].strip(),
                            "ask_volume": parts[10],
                            "ask_volume_change": parts[11]})
            utils.save(out_file, DAILY_VOLUME_HEADER, volumes)
        except Exception as e:
            logger.error("failed to get cffex daily trading volume", e)



