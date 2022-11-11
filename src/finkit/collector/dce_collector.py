import logging
import urllib.parse
import zipfile
from datetime import date
from io import BytesIO
from urllib import request

import bs4
from bs4 import BeautifulSoup

from .future_exchange_collector import FutureExchangeCollector, DAILY_VOLUME_HEADER, DAILY_STOCK_HEADER
from .. import utils

DCE_DAILY_VOLUME_URL = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html"
DCE_DAILY_VOLUME_FILE_FORMAT = "daily-volume-dce-{}.csv"
DCE_DAILY_STOCK_URL = "http://www.dce.com.cn/publicweb/quotesdata/wbillWeeklyQuotes.html"
DCE_DAILY_STOCK_FILE_FORMAT = "daily-stock-dce-{}.csv"

logger = logging.getLogger(__name__)


class DceCollector(FutureExchangeCollector):

    def __init__(self, file_path: str = "./data",
                 mydate: date = None, overwrite: bool = False, tz: int = 8) -> None:
        super().__init__(file_path, mydate, overwrite, tz)

    def crawl_daily_stock(self):
        out_file = self.file_path + "/" + DCE_DAILY_STOCK_FILE_FORMAT.format(self.mydate.isoformat())
        if self.file_exists(out_file) and self.mydate_is_weekend():
            return
        form_data = {
            "year": self.mydate.year,
            "month": self.mydate.month - 1,
            "day": self.mydate.day,
            "wbillWeeklyQuotes.variety": "all"
        }
        try:
            with request.urlopen(DCE_DAILY_STOCK_URL,
                                 data=urllib.parse.urlencode(form_data).encode()) as fp:
                soup = BeautifulSoup(fp.read(), features='lxml')
                # print(soup.find("table").prettify())
                table = soup.find("table")
                if table is None:
                    logger.warning("dce daily stock table not available")
                    return
                stocks = []
                for tr in table.contents:
                    if isinstance(tr, bs4.Tag):
                        tds = list(tr.stripped_strings)
                        if len(tds) == 5 and tds[0] != "品种":
                            stocks.append({
                                "varname": tds[0],
                                "warehouse": tds[1],
                                "stock": tds[3],
                                "change": tds[4],
                                "unit": "手", "unit_multiplier": 1})
                utils.save(out_file, DAILY_STOCK_HEADER, stocks)
        except Exception as e:
            logger.error("failed to download dce daily stock", e)

    def crawl_daily_volume(self):
        out_file = self.file_path + "/" + DCE_DAILY_VOLUME_FILE_FORMAT.format(self.mydate.isoformat())
        if self.file_exists(out_file) and self.mydate_is_weekend():
            return
        form_data = {
            "year": self.mydate.year,
            "month": self.mydate.month - 1,
            "day": self.mydate.day,
            "batchExportFlag": "batch"
        }
        try:
            with request.urlopen(DCE_DAILY_VOLUME_URL,
                                 data=urllib.parse.urlencode(form_data).encode()) as fp,\
                    zipfile.ZipFile(BytesIO(fp.read())) as zfile:
                trading_volumes = []
                for filename in zfile.namelist():
                    instrument = filename.split("_")[1]
                    volumes = []
                    # total_volumes = []
                    # if instrument != "i2301":
                    #     continue
                    # logger.info("processing instrument: %s", instrument)
                    for line in zfile.read(filename).splitlines():
                        parts = line.decode("utf-8").split()
                        if len(parts) >= 3:
                            if parts[0].isdigit():
                                volumes.append({
                                    "rank": int(parts[0]),
                                    "part_name": parts[1],
                                    "volume": int(parts[2].replace(",", "")),
                                    "change": int(parts[3].replace(",", ""))
                                })
                    batch_size = int(len(volumes) / 3)
                    if batch_size * 3 == len(volumes):
                        for i in range(batch_size):
                            trading_volumes.append({
                                "instrument": instrument,
                                "rank": volumes[i]["rank"],
                                "part_name_1": volumes[i]["part_name"],
                                "trading_volume": volumes[i]["volume"],
                                "trading_volume_change": volumes[i]["change"],
                                "part_name_2": volumes[i + batch_size]["part_name"],
                                "bid_volume": volumes[i + batch_size]["volume"],
                                "bid_volume_change": volumes[i + batch_size]["change"],
                                "part_name_3": volumes[i + batch_size * 2]["part_name"],
                                "ask_volume": volumes[i + batch_size * 2]["volume"],
                                "ask_volume_change": volumes[i + batch_size * 2]["change"]
                            })
                utils.save(out_file, DAILY_VOLUME_HEADER, trading_volumes)
        except Exception as e:
            logger.error("failed to download dce daily volume", e)
