import logging
from logging import INFO

from aggregator import Aggregator

if __name__ == "__main__":
    logging.basicConfig(level=INFO)

    agg = Aggregator()
    s = '2018-07-15'
    e = '2018-07-15'
    portfolio = agg.build_portfolio(agg.get_date_range(s, e))
    debug = True
