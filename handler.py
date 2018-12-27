import json
import logging
import datetime
import boto3
from logging import INFO

from portfolio import Portfolio
from py_utils.portfolio_client import PortfolioClient

"""
Responsible for executing the portfolio builder.
"""
def run(event, context):
    logging.basicConfig(level=INFO)
    s = '2018-07-13'
    e = "{0:%Y-%m-%d}".format(datetime.datetime.now())
    s3 = boto3.client('s3', region_name='us-west-2')

    logging.info("Executing Portfolio Job {} "
                 "for Date Range {} - {}"
                 .format(json.dumps(event, indent=2), s, e))

    portfolio = Portfolio()
    client = PortfolioClient(s3)

    portfolio = portfolio.build_portfolio(portfolio.get_date_range(s, e))
    client.send_to_bucket(portfolio)