
import akshare as ak
import pytest
import finkit.collector.future_source as fs
from datetime import date
import pandas as pd

@pytest.mark.skip
def test_get_rank_table():
    df = ak.get_dce_rank_table(date="20230213")
    print(df)


def test_get_future_daily():
    # df = ak.get_futures_daily(start_date="20230210", end_date="20230222")
    # df = ak.get_gfex_daily("20221222")
    # df = ak.get_cffex_daily("20100416")
    # df = ak.get_shfe_daily("20020107")
    # df = fs.get_shfe_daily_vol_rank(mydate=date(2012, 1, 5))
    # df = fs.get_czce_daily_vol_rank(mydate=date(2017, 10, 10))
    # df = pd.DataFrame.from_dict()
    # df = fs.get_shfe_daily_vol_rank(mydate=date(2022, 1, 4))
    df = fs.get_dce_daily_vol_rank(mydate=date(2021, 1, 5))
    # df = fs.get_cffex_daily_vol_rank(mydate=date(2023, 3, 14))
    # df = fs.get_czce_daily_vol_rank(mydate=date(2019, 1, 1))
    # df = fs.get_daily_vol_rank(mydate=date(2023, 3, 1))
    print(df)
    # print("\n%s", df.to_string())
    # print(ak.get_czce_rank_table("20151008"))

