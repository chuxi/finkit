from datetime import date

from finkit import utils
from finkit.algorithm.huatai_option_algo import HuataiOptionAlgo


def test_huatai_option_algo():
    utils.logging_config("../logging.yml")
    algo = HuataiOptionAlgo("au2212.shfe",
                            ["2022-11-24", "2022-11-23"],
                            [396, 399],
                            rate=0.03, dividend_rate=0.03,
                            mydate=date(2022, 10, 25),
                            storage_directory="./data")
    print()
    print(algo.calc().to_string())
