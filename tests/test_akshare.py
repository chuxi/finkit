
import akshare as ak


def test_get_rank_table():
    df = ak.get_dce_rank_table(date="20230213")
    ak.futures_rule()
    print(df)

