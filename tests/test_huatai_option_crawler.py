from finkit import utils
from finkit.collector import HuaTaiOptionCrawler
from datetime import date as _date


def test_crawler():
    utils.logging_config("../logging.yml")
    cc = HuaTaiOptionCrawler(mydate=_date(2022, 10, 20), overwrite=True)
    cc.crawl()

