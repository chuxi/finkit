
import akshare as ak
import pytest


@pytest.mark.skip
def test_get_rank_table():
    df = ak.get_dce_rank_table(date="20230213")
    print(df)


def test_get_future_daily():
    df = ak.get_futures_daily(start_date="20230210", end_date="20230222")
    print("\n%s", df.to_string())

