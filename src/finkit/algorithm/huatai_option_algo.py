import csv
import logging
import os
from datetime import date, datetime, timezone, timedelta
import numpy as np
import scipy.stats as sps
import pandas as pd

from .algo import Algorithm, OPTION_DATAFRAME_INDEX
from ..collector.huatai_option_crawler import HuaTaiOptionCrawler

OPTION_FILE_FORMAT = "huatai-option-{}.csv"
OPTION_COEFF_FILE_FORMAT = "huatai-option-coeff-{}.csv"
HUATAI_TIMEZONE = timedelta(hours=8)

logger = logging.getLogger(__name__)


class HuataiOptionAlgo(Algorithm):
    """
    huatai 接口支持获取波动率计算参数，存储中间数据到临时文件，而后读取数据计算，
        若数据不存在，则拉取对应数据
    """

    def __init__(self,
                 contract: str,
                 strike_date: [],
                 strike_price: [],
                 rate: float = 0.03,
                 dividend_rate: float = 0,
                 mydate: date = None,
                 storage_directory: str = "./data"):
        """
        检查本地文件是否存在，若不存在则获取对应数据
        """
        if mydate is None:
            mydate = datetime.now(timezone(HUATAI_TIMEZONE)).date()
        # {source}-option-{date}.csv and {source}-option-coeff-{date}.csv file exists
        option_file = storage_directory + "/" + OPTION_FILE_FORMAT.format(mydate.isoformat())
        option_coeff_file = storage_directory + "/" + OPTION_COEFF_FILE_FORMAT.format(mydate.isoformat())
        if not os.path.exists(option_file):
            # try to download the data
            crawler = HuaTaiOptionCrawler(file_path=storage_directory, mydate=mydate)
            crawler.crawl()
        if not os.path.exists(option_coeff_file):
            logger.error("option coeff source file not exists: %s", option_coeff_file)
            return

        # contract code exists in the source
        option_data = None
        with open(option_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["contract_code"].startswith(contract.lower()):
                    option_data = row
                    break
        if option_data is None:
            logger.error("can not find the contract code in option data source file, contract: %s", contract)
            return
        option_coeff_data = []
        available_strike_date = []
        with open(option_coeff_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["contract_code"].startswith(contract.lower()):
                    available_strike_date.append(row["expiry_date"])
                    if row["expiry_date"] in strike_date:
                        option_coeff_data.append(row)
        if len(option_coeff_data) == 0:
            logger.error("can not find the contract code and expiry date in source file, "
                         "valid strike dates: %s", available_strike_date)
            return

        self.strike_date = np.array(strike_date, dtype="datetime64")
        # todo:: support hours time diff ???
        self.mytime = (self.strike_date - np.datetime64(mydate)) / np.timedelta64(365, 'D')
        self.mytime = np.reshape(self.mytime, (1, -1))

        self.last_price = float(option_data["last_price"])
        self.strike_price = np.array(strike_price, dtype=float).reshape(-1, 1)

        ask_coeff_a = np.array([float(x["ask_coeff_a"]) for x in option_coeff_data]).reshape(1, -1)
        ask_coeff_b = np.array([float(x["ask_coeff_b"]) for x in option_coeff_data]).reshape(1, -1)
        ask_coeff_c = np.array([float(x["ask_coeff_c"]) for x in option_coeff_data]).reshape(1, -1)

        bid_coeff_a = np.array([float(x["bid_coeff_a"]) for x in option_coeff_data]).reshape(1, -1)
        bid_coeff_b = np.array([float(x["bid_coeff_b"]) for x in option_coeff_data]).reshape(1, -1)
        bid_coeff_c = np.array([float(x["bid_coeff_c"]) for x in option_coeff_data]).reshape(1, -1)

        ks = self.strike_price / self.last_price
        ask_sigma = (ask_coeff_a * ks ** 2 + ask_coeff_b * ks + ask_coeff_c) / 100
        bid_sigma = (bid_coeff_a * ks ** 2 + bid_coeff_b * ks + bid_coeff_c) / 100

        self.sigma = np.vstack((ask_sigma, bid_sigma))
        self.strike_price = np.vstack((self.strike_price, self.strike_price))

        self.rate = rate
        self.dividend_rate = dividend_rate

        logger.info("\nhuatai option %s, last price: %s \n", contract, self.last_price)

    def calc(self) -> pd.DataFrame:
        """
        :return: dataframe, columns:
         [ contract, side, strike price, current date, strike date,
            rate, dividend rate, delta, option price ]
        """
        d1 = (np.log(self.last_price / self.strike_price) +
              (self.rate - self.dividend_rate + 0.5 * self.sigma ** 2) * self.mytime) / self.sigma / np.sqrt(
            self.mytime)
        d2 = d1 - self.sigma * np.sqrt(self.mytime)
        call = self.last_price * np.exp(-self.dividend_rate * self.mytime) * sps.norm.cdf(d1) - \
               self.strike_price * np.exp(-self.rate * self.mytime) * sps.norm.cdf(d2)
        put = self.strike_price * np.exp(-self.rate * self.mytime) * sps.norm.cdf(-d2) -\
              self.last_price * np.exp(-self.dividend_rate * self.mytime) * sps.norm.cdf(-d1)
        row = int(self.strike_price.shape[0] / 2)
        col = int(self.mytime.shape[1])
        result = []
        for j in range(col):
            for i in range(row):
                result.append([
                    self.strike_date[j],
                    self.last_price,
                    self.strike_price[i, 0],
                    self.mytime[0, j],
                    self.sigma[i, j],
                    self.sigma[i + row, j],
                    call[i, j],
                    call[i + row, j],
                    put[i, j],
                    put[i + row, j]
                ])

        return pd.DataFrame(data=result, columns=OPTION_DATAFRAME_INDEX)
