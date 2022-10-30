import json
import logging
import os
from datetime import date
from urllib import request

from .crawler import Crawler, DAILY_TRADING_VOLUME_HEADER
from .. import utils

SHFE_DAILY_TRADING_VOLUME_URL = "https://www.shfe.com.cn/data/dailydata/kx/pm{}.dat"
SHFE_DAILY_TRADING_VOLUME_FILE_FORMAT = "daily-trading-volume-shfe-{}.csv"

logger = logging.getLogger(__name__)


class ShfeDailyTradingVolumeCrawler(Crawler):

    def __init__(self,
                 file_path: str = "./data",
                 mydate: date = None,
                 overwrite: bool = False):
        self.file_path = file_path
        self.mydate = mydate
        self.overwrite = overwrite
        logging.info("crawling shfe daily trading volume at date: %s", self.mydate.isoformat())

    def crawl(self):
        cal = self.mydate.weekday()
        if cal == 5 or cal == 6:
            logging.info("skipping weekends: %s", self.mydate.isocalendar())
            return
        option_file = self.file_path + "/" + SHFE_DAILY_TRADING_VOLUME_FILE_FORMAT.format(self.mydate.isoformat())
        if os.path.exists(option_file) and not self.overwrite:
            logging.info("data file exists, return")
            return
        url = SHFE_DAILY_TRADING_VOLUME_URL.format(self.mydate.strftime("%Y%m%d"))
        with request.urlopen(url) as f:
            body = f.read()
            data = json.loads(body.decode("utf-8"))["o_cursor"]
            if data is None or not isinstance(data, list):
                logger.error("failed to get data, response: %s", body)
                return
            volume_data = []
            for el in data:
                volume_data.append({
                    "instrument": el["INSTRUMENTID"].strip(),
                    "rank": el["RANK"],
                    "part_name_1": el["PARTICIPANTABBR1"].strip(),
                    "part_id_1": el["PARTICIPANTID1"].strip(),
                    "trading_volume": el["CJ1"],
                    "trading_volume_change": el["CJ1_CHG"],
                    "part_name_2": el["PARTICIPANTABBR2"].strip(),
                    "part_id_2": el["PARTICIPANTID2"].strip(),
                    "bid_volume": el["CJ2"],
                    "bid_volume_change": el["CJ2_CHG"],
                    "part_name_3": el["PARTICIPANTABBR3"].strip(),
                    "part_id_3": el["PARTICIPANTID3"].strip(),
                    "ask_volume": el["CJ3"],
                    "ask_volume_change": el["CJ3_CHG"]
                })
            utils.save(option_file, DAILY_TRADING_VOLUME_HEADER, volume_data)
