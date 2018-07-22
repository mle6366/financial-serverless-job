import functools
import math
import unittest
from unittest.mock import MagicMock

import pandas as pd

from portfolio import Portfolio


class TestPortfolio(unittest.TestCase):

    def setUp(self):
        pass

    def get_dummy_holdings(self, date_time_index=None):
        df = pd.DataFrame(index=date_time_index)
        df2 = pd.read_csv("tests/data/holdings.csv",
                          index_col='timestamp',
                          parse_dates=True,
                          na_values=['nan'])
        df = df.join(df2)
        df.sort_index(ascending=False, inplace=True)
        return df

    def get_dummy_stock(self, symbol, date_time_index, columns):
        default_range = pd.date_range('2018-05-04', '2018-04-01')
        df_start = pd.DataFrame(index=date_time_index if date_time_index is not None else default_range)
        df = pd.read_csv("tests/data/{}.csv".format(symbol),
                         index_col='timestamp',
                         parse_dates=True,
                         usecols=columns)
        return df_start.join(df)

    def get_dummy_starter_dataframe(self, date_time_index, cols):
        return self.get_dummy_stock('SPY', date_time_index, cols).dropna()

    def get_dummy_composite_dataframe(self, date_time_index):
        symbols = list(self.get_dummy_holdings(date_time_index).columns.values)
        cols = ['timestamp', 'close']
        df = self.get_dummy_starter_dataframe(date_time_index, cols)
        df = df.rename(columns={'close': 'SPY'})
        for symbol in symbols:
            if symbol == 'timestamp' or symbol == 'SPY':
                continue
            df_tmp = self.get_dummy_stock(symbol, date_time_index, cols)
            df_tmp = df_tmp.rename(columns={'close': symbol})
            df = df.join(df_tmp)
        return df

    def test_starter_dataframe(self):
        mock_client = MagicMock()
        mock_client.get_stock = self.get_dummy_stock
        agg = Portfolio(mock_client)
        s = '2017-12-11'
        e = '2018-04-29'
        cols = ['timestamp', 'open', 'close']
        date_range = agg.get_date_range(s, e)
        df = agg.get_starter_dataframe(date_range, cols)
        expected_headers = ['open', 'close']
        self.assertEqual(expected_headers, list(df.columns.values))

    def test_calculate_position_values(self):
        mock_client = MagicMock()
        s = '2018-03-31'
        e = '2018-05-04'
        date_range = pd.date_range(s, e)
        agg = Portfolio(mock_client)
        mock_client.get_holdings = self.get_dummy_holdings
        mock_client.get_stock = self.get_dummy_stock
        df = self.get_dummy_composite_dataframe(date_range)
        result = agg.calculate_position_values(df, date_range)
        expected_headers = ['AMZN', 'GOOG', 'NVDA', 'NXPI']

        amzn_expected = 6.0 * 1580.95
        amzn_calculated = result.ix['2018-05-04', ['AMZN']].get('AMZN')

        nxp_expected_one = 10.0 * 112.9900
        nxp_calculated_one = result.ix['2018-04-18', ['NXPI']].get('NXPI')

        nxp_expected_two = 5.0 * 113.16
        nxp_calculated_two = result.ix['2018-04-17', ['NXPI']].get('NXPI')

        no_holdings = result.ix['2018-04-04', ['NXPI']].get('NXPI')

        self.assertEqual(expected_headers, list(result.columns.values))
        self.assertEqual(amzn_expected, amzn_calculated)
        self.assertEqual(nxp_expected_one, nxp_calculated_one)
        self.assertEqual(nxp_expected_two, nxp_calculated_two)
        self.assertTrue(math.isnan(no_holdings))

    def test_build_portflio(self):
        mock_client = MagicMock()
        s = '2018-03-31'
        e = '2018-05-04'
        date_range = pd.date_range(s, e)
        agg = Portfolio(mock_client)
        mock_client.get_holdings = self.get_dummy_holdings
        mock_client.get_stock = self.get_dummy_stock
        result = agg.build_portfolio(date_range)
        # stock_val * holdings
        exp_amzn = 1580.9500 * 6
        exp_goog = 1048.2100 * 20
        exp_nvda = 239.0600 * 100
        exp_nxpi = 100.2800 * 10
        expected_last = functools.reduce(lambda a, b: a + b, list([exp_amzn, exp_goog, exp_nvda, exp_nxpi]))
        self.assertEqual(expected_last, result.tail(1).get('2018-05-04'))

    def test_cumulative_returns(self):
        mock_client = MagicMock()
        s = '2018-03-31'
        e = '2018-04-30'
        date_range = pd.date_range(s, e)
        agg = Portfolio(mock_client)
        mock_client.get_holdings = self.get_dummy_holdings
        mock_client.get_stock = self.get_dummy_stock
        result = agg.build_portfolio(date_range)

        expected_trade_days = 21  # trade days in april 2018
        actual_trade_days = result.values.shape[0]
        self.assertEqual(expected_trade_days, actual_trade_days)
        self.assertEqual(1, result.values.ndim)  # portfolio is a series
        self.assertEqual(50858.17999999999, result['2018-04-09'])
        self.assertEqual(53282.380000000005, result['2018-04-30'])
