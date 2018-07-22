import logging
from logging import INFO

from portfolio import Portfolio

if __name__ == "__main__":
    logging.basicConfig(level=INFO)

    portfolio = Portfolio()
    s = '2018-07-13'
    e = '2018-07-22'
    portfolio = portfolio.build_portfolio(portfolio.get_date_range(s, e))
    debug = True
