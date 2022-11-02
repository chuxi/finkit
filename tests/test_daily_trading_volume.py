import pytest

from finkit import utils
from finkit.collector import ShfeDailyTradingVolumeCrawler
from finkit.collector import DceDailyTradingVolumeCrawler
from finkit.collector import CzceDailyTradingVolumeCrawler
from finkit.collector import CffexDailyTradingVolumeCrawler
from datetime import date

utils.logging_config("../logging.yml")


@pytest.mark.skip
def test_shfe():
    ShfeDailyTradingVolumeCrawler(mydate=date(2022, 10, 31), overwrite=True).crawl()


@pytest.mark.skip
def test_dce():
    DceDailyTradingVolumeCrawler(mydate=date(2022, 10, 31), overwrite=True).crawl()


# @pytest.mark.skip
def test_czce():
    CzceDailyTradingVolumeCrawler(mydate=date(2022, 10, 31), overwrite=True).crawl()


@pytest.mark.skip
def test_cffex():
    CffexDailyTradingVolumeCrawler(mydate=date(2022, 11, 28), overwrite=True).crawl()
