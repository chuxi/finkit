import json
import logging
import os
from datetime import date
from urllib import request

from .crawler import Crawler, DAILY_STOCK_HEADER
from .. import utils

SHFE_DAILY_STOCK_URL = "https://www.shfe.com.cn/data/dailydata/{}dailystock.dat"
SHFE_DAILY_STOCK_FILE_FORMAT = "daily-stock-shfe-{}.csv"

logger = logging.getLogger(__name__)


class ShfeDailyStockCrawler(Crawler):

    def __init__(self,
                 file_path: str = "./data",
                 mydate: date = None,
                 overwrite: bool = False):
        self.file_path = file_path
        self.mydate = mydate
        self.overwrite = overwrite
        logging.info("crawling shfe daily stock at date: %s", self.mydate.isoformat())

    def crawl(self):
        cal = self.mydate.weekday()
        if cal == 5 or cal == 6:
            logging.info("skipping weekends: %s", self.mydate.isocalendar())
            return
        out_file = self.file_path + "/" + SHFE_DAILY_STOCK_FILE_FORMAT.format(self.mydate.isoformat())
        if os.path.exists(out_file) and not self.overwrite:
            logging.info("data file exists, return")
            return
        url = SHFE_DAILY_STOCK_URL.format(self.mydate.strftime("%Y%m%d"))

        try:
            with request.urlopen(url) as f:
                body = f.read()
                data = json.loads(body.decode("utf-8"))["o_cursor"]
                if data is None or not isinstance(data, list):
                    logger.error("failed to get data, response: %s", body)
                    return
                results = []
                for el in data:
                    results.append({
                        "varname": el["VARNAME"].split("$$")[0],
                        "region": el["REGNAME"].split("$$")[0],
                        "warehouse": el["WHABBRNAME"].split("$$")[0],
                        "stock": int(el["WRTWGHTS"]),
                        "change": int(el["WRTCHANGE"]),
                        "unit": self._parse_unit(el["WGHTUNIT"])
                    })
                utils.save(out_file, DAILY_STOCK_HEADER, results)
        except Exception as e:
            logger.error("shfe daily stock failed: %s, %s", url, e)

    def _parse_unit(self, ut: str) -> str:
        match ut:
            case "0":
                return "克"
            case "1":
                return "千克"
            case "2":
                return "吨"
            case "3":
                return "桶"
            case _:
                return ""
