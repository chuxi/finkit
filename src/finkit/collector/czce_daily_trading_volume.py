import json
import logging
import os
from datetime import date
from urllib import request
from urllib.error import HTTPError

from .crawler import Crawler, DAILY_TRADING_VOLUME_HEADER
from .. import utils

CZCE_URL = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataHolding.txt"
CZCE_FILE_FORMAT = "daily-trading-volume-czce-{}.csv"
DOWNLOAD_FILE_FORMAT = "daily-trading-volume-czce-{}.txt"

logger = logging.getLogger(__name__)


class CzceDailyTradingVolumeCrawler(Crawler):

    def __init__(self,
                 file_path: str = "./data",
                 mydate: date = None,
                 overwrite: bool = False):
        self.file_path = file_path
        self.mydate = mydate
        self.overwrite = overwrite
        logging.info("crawling czce daily trading volume at date: %s", self.mydate.isoformat())

    def crawl(self):
        cal = self.mydate.weekday()
        if cal == 5 or cal == 6:
            logging.info("skipping weekends: %s", self.mydate.isocalendar())
            return
        out_file = self.file_path + "/" + CZCE_FILE_FORMAT.format(self.mydate.isoformat())
        if os.path.exists(out_file) and not self.overwrite:
            logging.info("data file exists, return")
            return
        url = CZCE_URL.format(self.mydate.year, self.mydate.strftime("%Y%m%d"))
        download_file = self.file_path + "/" + DOWNLOAD_FILE_FORMAT.format(self.mydate.isoformat())

        def extract_num(e: str) -> int:
            s = e.lstrip("|")
            if s == "-":
                return 0
            else:
                return int(s.replace(",", ""))

        try:
            request.urlretrieve(url, download_file)
            instrument = ""
            volumes = []
            with open(download_file, "r", encoding="utf-8") as f:
                for line in f.readlines():
                    parts = line.split()
                    if len(parts) > 1:
                        if parts[0].isdigit():
                            volumes.append({
                                "instrument": instrument,
                                "rank": parts[0],
                                "part_name_1": parts[1].lstrip("|"),
                                "trading_volume": extract_num(parts[2]),
                                "trading_volume_change": extract_num(parts[3]),
                                "part_name_2": parts[4].lstrip("|"),
                                "bid_volume": extract_num(parts[5]),
                                "bid_volume_change": extract_num(parts[6]),
                                "part_name_3": parts[7].lstrip("|"),
                                "ask_volume": extract_num(parts[8]),
                                "ask_volume_change": extract_num(parts[9])
                            })
                        elif parts[0].startswith("品种："):
                            instrument = parts[0].split("：")[1]
            utils.save(out_file, DAILY_TRADING_VOLUME_HEADER, volumes)
        except HTTPError:
            logger.warning("czce download daily trading volume failed")

        if os.path.exists(download_file):
            os.remove(download_file)