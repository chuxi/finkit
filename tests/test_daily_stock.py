import pytest

from finkit import utils
from finkit.collector import ShfeDailyStockCrawler, CollectorContext
from finkit.collector import future_source, future_job
from datetime import date

utils.logging_config("../logging.yml")


@pytest.mark.skip
def test_shfe():
    ShfeDailyStockCrawler(mydate=date(2022, 11, 1), overwrite=True).crawl()


def test_daily_kline():
    context = CollectorContext(config_file="../config.toml")
    future_job.daily_kline(context, mydate=date(2019, 3, 8))


