import pytest

from finkit import utils
from finkit.collector import ShfeDailyTradingVolumeCrawler
from finkit.collector import DceDailyTradingVolumeCrawler
from datetime import date

utils.logging_config("../logging.yml")


@pytest.mark.skip
def test_shfe():
    cc = ShfeDailyTradingVolumeCrawler(mydate=date(2022, 10, 28), overwrite=True)
    cc.crawl()


@pytest.mark.skip
def test_dce():
    cc = DceDailyTradingVolumeCrawler(mydate=date(2022, 11, 2), overwrite=True)
    cc.crawl()
