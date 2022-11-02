import logging
import os
import urllib.parse
import zipfile
from datetime import date
from urllib import request

from .crawler import Crawler, DAILY_TRADING_VOLUME_HEADER
from .. import utils

DCE_DAILY_TRADING_VOLUME_URL = "http://www.dce.com.cn/publicweb/quotesdata/exportMemberDealPosiQuotesBatchData.html"
DCE_DAILY_TRADING_VOLUME_FILE_FORMAT = "daily-trading-volume-dce-{}.csv"
DOWNLOAD_FILE_FORMAT = "daily-trading-volume-dce-{}.zip"

logger = logging.getLogger(__name__)


class DceDailyTradingVolumeCrawler(Crawler):

    def __init__(self,
                 file_path: str = "./data",
                 mydate: date = None,
                 overwrite: bool = False):
        self.file_path = file_path
        self.mydate = mydate
        self.overwrite = overwrite
        logging.info("crawling dce daily trading volume at date: %s", self.mydate.isoformat())

    def crawl(self):
        cal = self.mydate.weekday()
        if cal == 5 or cal == 6:
            logging.info("skipping weekends: %s", self.mydate.isocalendar())
            return
        out_file = self.file_path + "/" + DCE_DAILY_TRADING_VOLUME_FILE_FORMAT.format(self.mydate.isoformat())
        if os.path.exists(out_file) and not self.overwrite:
            logging.info("data file exists, return")
            return
        form_data = {
            "year": self.mydate.year,
            "month": self.mydate.month - 1,
            "day": self.mydate.day,
            "batchExportFlag": "batch"
        }
        download_file = self.file_path + "/" + DOWNLOAD_FILE_FORMAT.format(self.mydate.isoformat())
        request.urlretrieve(DCE_DAILY_TRADING_VOLUME_URL, download_file,
                            reporthook=None, data=urllib.parse.urlencode(form_data).encode())
        try:
            download_zipfile = zipfile.ZipFile(download_file)
            trading_volumes = []
            for filename in download_zipfile.namelist():
                instrument = filename.split("_")[1]
                volumes = []
                # total_volumes = []
                # if instrument != "i2301":
                #     continue
                # logger.info("processing instrument: %s", instrument)
                for line in download_zipfile.read(filename).splitlines():
                    parts = line.decode("utf-8").split()
                    if len(parts) >= 3:
                        if parts[0].isdigit():
                            volumes.append({
                                "rank": int(parts[0]),
                                "part_name": parts[1],
                                "volume": int(parts[2].replace(",", "")),
                                "change": int(parts[3].replace(",", ""))
                            })
                        # elif parts[0] == "总计":
                        #     volumes.append({
                        #         "rank": 999,
                        #         "part_name": "总计",
                        #         "volume": int(parts[1].replace(",", "")),
                        #         "change": int(parts[2].replace(",", "")),
                        #     })
                        # elif parts[0] == "期货公司会员":
                        #     total_volumes.append({
                        #         "rank": 0,
                        #         "part_name": "期货公司",
                        #         "volume": int(parts[1].replace(",", "")),
                        #         "change": int(parts[2].replace(",", "")),
                        #     })
                        # print(parts)
                    # if len(parts) > 1 and parts[0] == "名次":
                    #     has_record = True
                    # if len(parts)
                    #     print(parts)
                # break
                # if len(total_volumes) == 3:
                #     trading_volumes.append({
                #         "instrument": instrument,
                #         "rank": total_volumes[0]["rank"],
                #         "part_name_1": total_volumes[0]["part_name"],
                #         "trading_volume": total_volumes[0]["volume"],
                #         "trading_volume_change": total_volumes[0]["change"],
                #         "part_name_2": total_volumes[1]["part_name"],
                #         "bid_volume": total_volumes[1]["volume"],
                #         "bid_volume_change": total_volumes[1]["change"],
                #         "part_name_3": total_volumes[2]["part_name"],
                #         "ask_volume": total_volumes[2]["volume"],
                #         "ask_volume_change": total_volumes[2]["change"]
                #     })
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
            utils.save(out_file, DAILY_TRADING_VOLUME_HEADER, trading_volumes)
            download_zipfile.close()
        except zipfile.BadZipFile:
            logger.error("no downloaded file in dce daily trading volume")

        # 删除下载的文件
        if os.path.exists(download_file):
            os.remove(download_file)
