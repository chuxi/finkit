import json
import logging
from datetime import date
from urllib import request

from .future_exchange_collector import FutureExchangeCollector, DAILY_STOCK_HEADER, DAILY_VOLUME_HEADER
from .. import utils

SHFE_DAILY_STOCK_URL = "https://www.shfe.com.cn/data/dailydata/{}dailystock.dat"
SHFE_DAILY_STOCK_FILE_FORMAT = "daily-stock-shfe-{}.csv"

SHFE_DAILY_VOLUME_URL = "https://www.shfe.com.cn/data/dailydata/kx/pm{}.dat"
SHFE_DAILY_VOLUME_FILE_FORMAT = "daily-volume-shfe-{}.csv"

logger = logging.getLogger(__name__)


class ShfeCollector(FutureExchangeCollector):

    def __init__(self,
                 file_path: str = "./data",
                 mydate: date = None,
                 overwrite: bool = False) -> None:
        super().__init__(file_path, mydate, overwrite)

    def crawl_daily_stock(self):
        out_file = self.file_path + "/" + SHFE_DAILY_STOCK_FILE_FORMAT.format(self.mydate.isoformat())
        if self.file_exists(out_file) and self.mydate_is_weekend():
            return

        def _parse_unit(ut: str) -> str:
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
                        "unit": _parse_unit(el["WGHTUNIT"])
                    })
                utils.save(out_file, DAILY_STOCK_HEADER, results)
        except Exception as e:
            logger.error("shfe daily stock failed: %s, %s", url, e)

    def crawl_daily_volume(self):
        out_file = self.file_path + "/" + SHFE_DAILY_VOLUME_FILE_FORMAT.format(self.mydate.isoformat())
        if self.file_exists(out_file) and self.mydate_is_weekend():
            return
        url = SHFE_DAILY_VOLUME_URL.format(self.mydate.strftime("%Y%m%d"))
        with request.urlopen(url) as f:
            body = f.read()
            data = json.loads(body.decode("utf-8"))["o_cursor"]
            if data is None or not isinstance(data, list):
                logger.error("failed to get data, response: %s", body)
                return
            volume_data = []
            for el in data:
                if el["RANK"] == 999:
                    continue
                volume_data.append({
                    "instrument": el["INSTRUMENTID"].strip(),
                    "rank": el["RANK"],
                    "part_name_1": el["PARTICIPANTABBR1"].strip(),
                    # "part_id_1": el["PARTICIPANTID1"].strip(),
                    "trading_volume": el["CJ1"],
                    "trading_volume_change": el["CJ1_CHG"],
                    "part_name_2": el["PARTICIPANTABBR2"].strip(),
                    # "part_id_2": el["PARTICIPANTID2"].strip(),
                    "bid_volume": el["CJ2"],
                    "bid_volume_change": el["CJ2_CHG"],
                    "part_name_3": el["PARTICIPANTABBR3"].strip(),
                    # "part_id_3": el["PARTICIPANTID3"].strip(),
                    "ask_volume": el["CJ3"],
                    "ask_volume_change": el["CJ3_CHG"]
                })
            utils.save(out_file, DAILY_VOLUME_HEADER, volume_data)


