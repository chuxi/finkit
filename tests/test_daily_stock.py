import pytest

from finkit import utils
from finkit.collector import ShfeDailyStockCrawler
from datetime import date

utils.logging_config("../logging.yml")


# @pytest.mark.skip
def test_shfe():
    ShfeDailyStockCrawler(mydate=date(2022, 11, 1), overwrite=True).crawl()

