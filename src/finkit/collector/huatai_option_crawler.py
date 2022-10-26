import json
import logging
import os.path
from datetime import datetime, timezone, timedelta, date as _date
from urllib import request

from . import crawler
from .. import utils

OPTION_MARKET_PRICE_URL = "http://crm.htoption.cn/market_data?list="
OPTION_EXPIRY_DATE = "http://www.htoption.cn/weixin/app/index.php?i=4&c=entry&do=getexpiryday&m=ht_otc&mounth="
OPTION_VOL_URL_V1 = "http://www.htoption.cn/weixin/app/index.php?i=4&c=entry&do=getatmvol&m=ht_otc&mounth="
OPTION_VOL_URL_V2 = "http://www.htoption.cn/weixin/app/index.php?i=4&c=entry&do=getvolsurface&m=ht_otc&ExchangeCode="
OPTION_FILE_FORMAT = "huatai-option-{}.csv"
OPTION_COEFF_FILE_FORMAT = "huatai-option-coeff-{}.csv"

SINA_EXCHANGE_CODE_TABLE = {
    # "CFE": "CFFEX",
    "SHF": "SHFE",
    "CZC": "CZCE",
    "DCE": "DCE",
    "INE": "INE"
}
OPTION_HEADER = \
    ["contract_code", "product_name", "contract", "product_code",
     "date", "settle_price", "last_price", "close_yield",
     "multiplier", "unit", "price_limit", "ex_price_limit"]
OPTION_COEFFICIENT_HEADER = \
    ["contract_code", "expiry_date", "bid_coeff_a", "bid_coeff_b", "bid_coeff_c",
     "ask_coeff_a", "ask_coeff_b", "ask_coeff_c", "mid_coeff_a", "mid_coeff_b", "mid_coeff_c"]


class HuaTaiOptionCrawler(crawler.Crawler):
    """
    获取华泰期权的最新价格与波动率数据，close time
    """

    def __init__(self, file_path: str = "./",
                 mydate: _date = None,
                 tz: int = 8, overwrite: bool = False):
        """
        :param date: 爬取数据所在的日期
        :param tz: 日期时区
        """
        self.file_path = file_path
        self.tz = timezone(timedelta(hours=tz))
        self.date = mydate
        if not file_path.endswith("/"):
            file_path = file_path + "/"
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        self.file_path = file_path
        self.overwrite = overwrite
        logging.info("crawling huatai data source at date: %s", self.date.isoformat())

    def crawl(self):
        # 1. 检查该日期是否有数据，仅在交易日结束时有数据
        #  1.1 判断工作日
        cal = self.date.weekday()
        if cal == 5 or cal == 6:
            logging.info("skipping weekends: %s", self.date.isocalendar())
            return
        option_file = self.file_path + OPTION_FILE_FORMAT.format(self.date.isoformat())
        if os.path.exists(option_file) and not self.overwrite:
            logging.info("data file exists, return")
            return
        #  1.2 request接口查询是否有期权数据判断
        url = OPTION_EXPIRY_DATE + self.date.isoformat()
        with request.urlopen(url) as f:
            body = f.read()
            expiry_date_obj = json.loads(body.decode("utf-8"))
            expiry_date = datetime.strptime(expiry_date_obj["Date"], "%Y/%m/%d")
            if not self.date == expiry_date.date():
                logging.info("it is not a valid trading date, skipped")
                return

        # 2. 获取今天close price，后面一段时间波动率以及交易对参数
        option_coefficient_data = []
        option_params = {}
        for ex in SINA_EXCHANGE_CODE_TABLE.keys():
            url = OPTION_VOL_URL_V2 + ex
            with request.urlopen(url) as f:
                body = json.loads(f.read().decode("utf-8"))
                for el in body:
                    if not el["link_contract"]:
                        continue
                    link_contracts = el["link_contract"].split(",")
                    for link_contract in link_contracts:
                        contract = (link_contract + "." + SINA_EXCHANGE_CODE_TABLE[ex]).lower()
                        option_coefficient_data.append({
                            "contract_code": contract,
                            "ask_coefficient": json.loads(el["ask_coefficient"])[link_contract],
                            "mid_coefficient": json.loads(el["mid_coefficient"])[link_contract],
                            "bid_coefficient": json.loads(el["bid_coefficient"])[link_contract]})

                        option_params[contract] = {
                            "contract_code": contract,
                            "product_name": None,
                            "product_code": el["product_code"],
                            "contract": link_contract,
                            "date": el["vol_date"],
                            "multiplier": float(el["contract_multiplier"]),
                            "unit": el["trade_unit"],
                            "price_limit": float(el["price_limit"]),
                            "ex_price_limit": float(el["ex_price_limit"])
                        }
        # parse coefficient data
        option_coefficient = []

        def extract_coeff(coeff_body):
            coeff_begin = datetime.strptime(coeff_body["begin"], "%Y/%m/%d")
            coeff_end = datetime.strptime(coeff_body["end"], "%Y/%m/%d")
            coeffs = []
            while coeff_begin <= coeff_end:
                my_date = coeff_begin.date().strftime("%Y/%m/%d")
                if my_date in coeff_body:
                    coeffs.append({
                        "expiry_date": coeff_begin.date().isoformat(),
                        "coeff": coeff_body[my_date],
                    })
                coeff_begin = coeff_begin + timedelta(days=1)
            return coeffs

        for data in option_coefficient_data:
            ask_coeffs = extract_coeff(data["ask_coefficient"])
            mid_coeffs = extract_coeff(data["mid_coefficient"])
            bid_coeffs = extract_coeff(data["bid_coefficient"])
            if len(ask_coeffs) != len(mid_coeffs) or len(ask_coeffs) != len(bid_coeffs):
                logging.error("coefficients extraction error")
                return
            for i in range(len(ask_coeffs)):
                ask_coeff_parts = ask_coeffs[i]["coeff"].split(",")
                mid_coeff_parts = mid_coeffs[i]["coeff"].split(",")
                bid_coeff_parts = bid_coeffs[i]["coeff"].split(",")

                option_coefficient.append({
                    "contract_code": data["contract_code"],
                    "expiry_date": ask_coeffs[i]["expiry_date"],
                    "ask_coeff_a": float(ask_coeff_parts[0]),
                    "ask_coeff_b": float(ask_coeff_parts[1]),
                    "ask_coeff_c": float(ask_coeff_parts[2]),
                    "mid_coeff_a": float(mid_coeff_parts[0]),
                    "mid_coeff_b": float(mid_coeff_parts[1]),
                    "mid_coeff_c": float(mid_coeff_parts[2]),
                    "bid_coeff_a": float(bid_coeff_parts[0]),
                    "bid_coeff_b": float(bid_coeff_parts[1]),
                    "bid_coeff_c": float(bid_coeff_parts[2]),
                })

        option_coefficient_file = self.file_path + \
            OPTION_COEFF_FILE_FORMAT.format(self.date.isoformat())
        utils.save(option_coefficient_file, OPTION_COEFFICIENT_HEADER, option_coefficient)

        # 3. 获取最新收盘价
        option_prices = []
        url = OPTION_MARKET_PRICE_URL + ",".join(option_params.keys())
        with request.urlopen(url) as f:
            body = json.loads(f.read().decode("utf-8")[1:-1])
            for param in option_params.values():
                if param["contract_code"] not in body:
                    logging.warning("missing contract code in market price: %s", param["contract_code"])
                    continue
                prices = body[param["contract_code"]]
                param["settle_price"] = prices["settleprice"]
                param["last_price"] = prices["lastprice"]
                param["close_yield"] = prices["closeyield"]
                option_prices.append(param)
        utils.save(option_file, OPTION_HEADER, option_prices)


if __name__ == '__main__':
    """
    module方式执行
    """
    utils.logging_config("./logging.yml")
    cc = HuaTaiOptionCrawler(mydate=_date(2022, 9, 30), overwrite=True)
    cc.crawl()
