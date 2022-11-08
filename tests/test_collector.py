import pytest

from finkit import utils
from finkit.collector.shfe_collector import ShfeCollector
from finkit.collector.dce_collector import DceCollector
from finkit.collector.czce_collector import CzceCollector
from finkit.collector.cffex_collector import CffexCollector
from datetime import date

utils.logging_config("../logging.yml")


@pytest.mark.skip
def test_shfe():
    cc = ShfeCollector(mydate=date(2022, 11, 7), overwrite=True)
    cc.crawl_daily_stock()
    cc.crawl_daily_volume()


# @pytest.mark.skip
def test_dce():
    cc = DceCollector(mydate=date(2022, 11, 7), overwrite=True)
    cc.crawl_daily_stock()
    # cc.crawl_daily_volume()


@pytest.mark.skip
def test_czce():
    cc = CzceCollector(mydate=date(2022, 11, 8), overwrite=True)
    cc.crawl_daily_stock()
    # cc.crawl_daily_volume()


@pytest.mark.skip
def test_cffex():
    cc = CffexCollector(mydate=date(2022, 11, 8), overwrite=True)
    cc.crawl_daily_volume()

