import unittest
from unittest.mock import MagicMock

from datalake import DataLake


class TestDataLake(unittest.TestCase):
    def setUp(self):
        pass

    def test_get_stock_raw(self):
        mock_bucket = MagicMock()
        mock_bucket.download_file.return_value = {}

        mock_s3 = MagicMock()
        mock_s3.Bucket.return_value = mock_bucket

        symbol = "HELO"

        dl = DataLake(client=mock_s3)
        path = dl.get_stock_raw(symbol=symbol)

        exp_src_path = "alphavantage/{}/data.csv".format(symbol.upper())
        exp_dst_path = "/tmp/alphavantage_{}_data.csv.csv".format(symbol)
        exp_bucket = "expansellc-datalake"

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

        df = dl.get_stock(symbol, ["timestamp", "high", "low"])

        self.assertIsNotNone(df)
        dl.get_stock_raw.assert_called_once_with(symbol=symbol)

        headers = list(df.columns.values)
        self.assertEqual(exp_headers, headers)
