import logging
import numpy as np
import scipy.stats as sps

from . import algo


logger = logging.getLogger(__name__)


class OptionAlgo(algo.Algorithm):
    """
    使用numpy计算某个交易日结束后，标的资产期权合约的价格
    1. 行权价（给定确定值或者范围）
    2. 到期日期（标的资产未来波动率存在的所有日期）
    3. 期权看涨看跌，买卖四个方向维度
    """

    def __init__(self,
                 last_price: float,
                 strike_price: float,
                 side: str,
                 sigma: float,
                 time: float,
                 rate: float,
                 dividend_rate: float):
        self.last_price = last_price
        self.strike_price = strike_price
        self.side = side
        self.sigma = sigma
        self.time = time
        self.rate = rate
        self.dividend_rate = dividend_rate

    def calc(self):
        d1 = (np.log(self.last_price / self.strike_price) +
              (self.rate - self.dividend_rate + 0.5 * self.sigma ** 2) * self.time) / self.sigma / np.sqrt(self.time)
        delta = 0.5
        if self.side.startswith("call-"):
            delta = np.exp(-self.dividend_rate * self.time) * sps.norm.cdf(d1)
        else:
            delta = np.exp(-self.dividend_rate * self.time) * (sps.norm.cdf(d1) - 1)
        if abs(delta) > 0.9 or abs(delta) < 0.1:
            # 该 行权价 偏离当前价格太多
            return 0

        d2 = d1 - self.sigma * np.sqrt(self.time)
        if self.side.startswith("call-"):
            return self.last_price * np.exp(-self.dividend_rate * self.time) * sps.norm.cdf(d1) - \
                   self.strike_price * np.exp(-self.rate * self.time) * sps.norm.cdf(d2)
        else:
            return self.strike_price * np.exp(-self.rate * self.time) * sps.norm.cdf(-d2) -\
                   self.last_price * np.exp(-self.dividend_rate * self.time) * sps.norm.cdf(-d1)

