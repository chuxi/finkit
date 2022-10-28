from finkit import utils
from finkit.collector import ShfeDailyTradingVolumeCrawler
from datetime import date


def test_crawler():
    utils.logging_config("../logging.yml")
    cc = ShfeDailyTradingVolumeCrawler(mydate=date(2022, 10, 28), overwrite=True)
    cc.crawl()


