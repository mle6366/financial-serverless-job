import boto3
import unittest
from portfolio_client import PortfolioClient
import pandas as pd


class TestPortfolioClient(unittest.TestCase):

    def setUp(self):
        pass

    def get_dummy_stock(self, symbol, date_time_index):
        default_range = pd.date_range('2018-04-01', '2018-04-30')
        df_start = pd.DataFrame(index=date_time_index if date_time_index is not None else default_range)
        df = pd.read_csv("tests/data/{}.csv".format(symbol),
                         index_col='timestamp',
                         parse_dates=True,
                         usecols=['timestamp', 'close'])
        return df_start.join(df)

    def test_s3_integration(self):
        s3 = boto3.client('s3', region_name='us-west-2')
        client = PortfolioClient(s3)
        client.send_to_bucket(self.get_dummy_stock('NVDA', None))
        print("stop here")
