import json
import logging
import re
from datetime import date
from io import BytesIO
from urllib import request
from urllib.error import HTTPError

from .future_exchange_collector import FutureExchangeCollector, DAILY_STOCK_HEADER, DAILY_VOLUME_HEADER
from .. import utils

CZCE_DAILY_STOCK_URL = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataWhsheet.txt"
CZCE_DAILY_STOCK_FILE_FORMAT = "daily-stock-czce-{}.csv"

CZCE_DAILY_VOLUME_URL = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/{}/{}/FutureDataHolding.txt"
CZCE_DAILY_VOLUME_FILE_FORMAT = "daily-volume-czce-{}.csv"

UNIT_PATTERN = re.compile(r"\d+")

logger = logging.getLogger(__name__)


class CzceCollector(FutureExchangeCollector):

    def __init__(self,
                 file_path: str = "./data",
                 mydate: date = None,
                 overwrite: bool = False) -> None:
        super().__init__(file_path, mydate, overwrite)

    def crawl_daily_stock(self):
        out_file = self.file_path + "/" + CZCE_DAILY_STOCK_FILE_FORMAT.format(self.mydate.isoformat())
        if self.file_exists(out_file) and self.mydate_is_weekend():
            return
        url = CZCE_DAILY_STOCK_URL.format(self.mydate.year, self.mydate.strftime("%Y%m%d"))
        try:
            with request.urlopen(url) as fp:
                instrument = ""
                volumes = []
                index1 = -1
                index2 = -1
                local_volumes = {}
                unit_multiplier = 1
                for line in fp:
                    parts = line.decode("utf-8").split()
                    if len(parts) > 1:
                        if parts[0].startswith("品种："):
                            instrument = parts[0].split("：")[1]
                            unit_multiplier = UNIT_PATTERN.findall(parts[3])[0]
                        elif parts[0].endswith("编号"):
                            for j in range(len(parts)):
                                if parts[j] == "|仓单数量" or parts[j] == "|仓单数量(完税)" or\
                                        parts[j] == "|预报数量":
                                    index1 = j
                                elif parts[j] == "|当日增减":
                                    index2 = j
                        elif parts[0] == "小计" and len(local_volumes) > 0:
                            local_volumes["change"] = parts[index2].lstrip("|")
                            local_volumes["stock"] = parts[index1].lstrip("|")
                            local_volumes["unit"] = "手"
                            local_volumes["unit_multiplier"] = unit_multiplier
                            volumes.append(local_volumes.copy())
                            local_volumes.clear()
                        elif parts[0].isdigit():
                            if len(local_volumes.keys()) > 0:
                                volumes.append(local_volumes.copy())
                                local_volumes.clear()
                            local_volumes["varname"] = instrument
                            local_volumes["warehouse"] = parts[1].lstrip("|")
                            local_volumes["stock"] = parts[index1].lstrip("|")
                            local_volumes["change"] = parts[index2].lstrip("|")
                            local_volumes["unit"] = "手"
                            local_volumes["unit_multiplier"] = unit_multiplier

                utils.save(out_file, DAILY_STOCK_HEADER, volumes)
        except HTTPError as e:
            logger.warning("czce download daily stock failed")
        except Exception as e:
            logger.error("czce daily stock failed", e)


    def crawl_daily_volume(self):
        out_file = self.file_path + "/" + CZCE_DAILY_VOLUME_FILE_FORMAT.format(self.mydate.isoformat())
        if self.file_exists(out_file) and self.mydate_is_weekend():
            return
        url = CZCE_DAILY_VOLUME_URL.format(self.mydate.year, self.mydate.strftime("%Y%m%d"))

        def extract_num(e: str) -> int:
            s = e.lstrip("|")
            if s == "-":
                return 0
            else:
                return int(s.replace(",", ""))

        try:
            with request.urlopen(url) as fp:
                instrument = ""
                volumes = []
                for line in fp:
                    parts = line.decode("utf-8").split()
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
                        elif parts[0].startswith("品种：") or parts[0].startswith("合约："):
                            instrument = parts[0].split("：")[1]
                utils.save(out_file, DAILY_VOLUME_HEADER, volumes)
        except HTTPError as e:
            logger.warning("czce download daily volume failed")
        except Exception as e:
            logger.error("czce daily volume failed", e)

