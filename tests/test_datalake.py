import math
import unittest
from unittest.mock import MagicMock

import boto3
import pandas as pd
from moto import mock_s3

from datalake import DataLake

BUCKET = "expansellc-datalake"


class TestDataLake(unittest.TestCase):
    def setUp(self):
        self.date_range = pd.date_range("2018-06-30", "2018-07-3")

    def test_get_stock_raw(self):
        mock_bucket = MagicMock()
        mock_bucket.download_file.return_value = {}

        mock_s3 = MagicMock()
        mock_s3.Bucket.return_value = mock_bucket

        symbol = "HELO"

        dl = DataLake(client=mock_s3)
        path = dl.get_stock_raw(symbol=symbol)

        exp_src_path = "alphavantage/{}/data.csv".format(symbol.upper())
        exp_dst_path = "/tmp/alphavantage_{}_data.csv.datalake".format(symbol)
        exp_bucket = BUCKET

        self.assertEqual(exp_dst_path, path)
        mock_bucket.download_file.assert_called_once_with(exp_src_path, exp_dst_path)
        mock_s3.Bucket.assert_called_once_with(exp_bucket)

    def test_get_stock(self):
        symbol = "NVDA"

        mock_s3 = MagicMock()
        dl = DataLake(client=mock_s3)
        dl.get_stock_raw = MagicMock(return_value="tests/data/{}.csv".format(symbol))

        df = dl.get_stock(symbol)

        self.assertIsNotNone(df)
        dl.get_stock_raw.assert_called_once_with(symbol=symbol)

        exp_headers = ["open", "high", "low", "close", "volume"]
        headers = list(df.columns.values)
        self.assertEqual(exp_headers, headers)

    def test_get_stock_custom(self):
        symbol = "NVDA"
        exp_headers = ["high", "low"]

        mock_s3 = MagicMock()
        dl = DataLake(client=mock_s3)
        dl.get_stock_raw = MagicMock(return_value="tests/data/{}.csv".format(symbol))

        df = dl.get_stock(symbol, columns=["timestamp", "high", "low"])

        self.assertIsNotNone(df)
        dl.get_stock_raw.assert_called_once_with(symbol=symbol)

        headers = list(df.columns.values)
        self.assertEqual(exp_headers, headers)

    @mock_s3
    def test_get_holding(self):
        symbol = "NVDA"
        exp_headers = [symbol]

        ### Setup virtual S3 data ###
        conn = boto3.resource("s3", region_name="us-west-2")
        conn.create_bucket(Bucket=BUCKET)
        hold = conn.Object(BUCKET, "personalcapital/holdings/{}.csv".format(symbol))
        hold.put(Body=open("tests/data/holdings/{}.csv".format(symbol), "rb"))

        dl = DataLake()
        df = dl.get_holding(symbol=symbol, date_range=pd.date_range("2018-07-01", "2018-07-03"))

        self.assertIsNotNone(df)
        headers = list(df.columns.values)
        self.assertEqual(exp_headers, headers)

        # must be 3 rows and 1 column
        self.assertEqual(3, df.values.shape[0])
        self.assertEqual(1, df.values.shape[1])
        self.assertEqual(float(10), df.values[0, 0])
        self.assertEqual(float(10), df.values[1, 0])
        self.assertEqual(float(10), df.values[2, 0])

    @mock_s3
    def test_get_holdings(self):
        ### Setup virtual S3 data ###
        conn = boto3.resource("s3", region_name="us-west-2")
        conn.create_bucket(Bucket=BUCKET)

        for s in ["AMZN", "GOOG", "NVDA"]:
            hold = conn.Object(BUCKET, "personalcapital/holdings/{}.csv".format(s))
            hold.put(Body=open("tests/data/holdings/{}.csv".format(s), "rb"))

        dl = DataLake()
        df = dl.get_holdings(date_range=self.date_range, ascending=True)
        self.assertIsNotNone(df)

        # must be 4 rows and 3 column
        self.assertEqual(4, df.values.shape[0])
        self.assertEqual(3, df.values.shape[1])
        # assert AMZN
        self.assertTrue(math.isnan(df.values[0, 0]))
        self.assertTrue(math.isnan(df.values[1, 0]))
        self.assertEqual(float(7), df.values[2, 0])
        self.assertEqual(float(7), df.values[3, 0])
        # assert GOOG
        self.assertTrue(math.isnan(df.values[0, 1]))
        self.assertEqual(float(5), df.values[1, 1])
        self.assertEqual(float(5), df.values[2, 1])
        self.assertEqual(float(5), df.values[3, 1])
        # assert NVDA
        self.assertTrue(math.isnan(df.values[0, 2]))
        self.assertEqual(float(10), df.values[1, 2])
        self.assertEqual(float(10), df.values[2, 2])
        self.assertEqual(float(10), df.values[3, 2])

    @mock_s3
    def test_get_holdings_date(self):
        ### Setup virtual S3 data ###
        conn = boto3.resource("s3", region_name="us-west-2")
        conn.create_bucket(Bucket=BUCKET)

        for s in ["AMZN", "GOOG", "NVDA"]:
            hold = conn.Object(BUCKET, "personalcapital/holdings/{}.csv".format(s))
            hold.put(Body=open("tests/data/holdings/{}.csv".format(s), "rb"))

        dl = DataLake()
        df = dl.get_holdings(date_range=pd.date_range("2018-07-01", "2018-07-01"))
        self.assertIsNotNone(df)

        # must be 1 rows and 3 column
        self.assertEqual(1, df.values.shape[0])
        self.assertEqual(3, df.values.shape[1])
        # assert AMZN
        self.assertTrue(math.isnan(df.values[0, 0]))
        # assert GOOG
        self.assertEqual(float(5), df.values[0, 1])
        # assert NVDA
        self.assertEqual(float(10), df.values[0, 2])
