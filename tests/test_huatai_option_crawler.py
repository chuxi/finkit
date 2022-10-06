from finkit import utils
from finkit.collector import HuaTaiOptionCrawler
from datetime import date as _date


def test_crawler():
    utils.logging_config("../logging.yml")
    cc = HuaTaiOptionCrawler(date=_date(2022, 10, 3), overwrite=True)
    cc.crawl()

