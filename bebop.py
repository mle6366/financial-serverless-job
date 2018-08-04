import logging
from logging import INFO
import boto3

from portfolio import Portfolio
from portfolio_client import PortfolioClient

if __name__ == "__main__":
    logging.basicConfig(level=INFO)
    s3 = boto3.client('s3', region_name='us-west-2')

    portfolio = Portfolio()
    portfolio_client = PortfolioClient(s3)
    s = '2018-07-13' # should we hardcode this as the earliest date?
    e = '2018-08-04'
    portfolio = portfolio.build_portfolio(portfolio.get_date_range(s, e))
    portfolio_client.send_to_bucket(portfolio)
    debug = True
