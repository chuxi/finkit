import logging

from finkit import utils
from finkit.algorithm import OptionAlgo


def test_option_algo():
    utils.logging_config("../logging.yml")
    algo = OptionAlgo(930, 900, "call-buy", 0.2, 1 / 6, 0.08, 0.03)
    logging.info("option price: %s", algo.calc())
    logging.info(OptionAlgo(20, 20, "put-buy", 0.25, 1 / 3, 0.09, 0.09).calc())


def test_huatai_option_algo():
    utils.logging_config("../logging.yml")
    logging.info(OptionAlgo(4544, 4400, "call-bid", 2.60020635, 0.04931507, 0.03, 0.03).calc())
