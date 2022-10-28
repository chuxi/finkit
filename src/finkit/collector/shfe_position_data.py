import logging
import os
from datetime import date

from .crawler import Crawler

SHFE_POSITION_DATA_URL = "https://www.shfe.com.cn/data/dailydata/kx/pm20221027.dat"
SHFE_POSITION_DATA_FILE_FORMAT = "shfe-"


logger = logging.getLogger(__name__)


class ShfePositionCrawler(Crawler):

    def __init__(self,
                 file_path: str = "./",
                 mydate: date = None,
                 overwrite: bool = False):
        self.file_path = file_path
        self.mydate = mydate
        self.file_path = file_path
        self.overwrite = overwrite
        logging.info("crawling huatai data source at date: %s", self.mydate.isoformat())

    def crawl(self):
        cal = self.mydate.weekday()
        if cal == 5 or cal == 6:
            logging.info("skipping weekends: %s", self.mydate.isocalendar())
            return
        option_file = self.file_path + SHFE_POSITION_DATA_FILE_FORMAT.format(self.mydate.isoformat())
        if os.path.exists(option_file) and not self.overwrite:
            logging.info("data file exists, return")
            return
        pass


