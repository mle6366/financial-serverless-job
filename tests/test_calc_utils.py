import unittest
import pandas as pd
from calc_utils import CalcUtils

class TestCalcUtils(unittest.TestCase):

    def setUp(self):
        pass

    def get_dummy_stock(self, symbol, date_time_index):
        default_range = pd.date_range('2018-04-30', '2018-04-01')
        df_start = pd.DataFrame(index=date_time_index if date_time_index is not None else default_range)
        df = pd.read_csv("tests/data/{}.csv".format(symbol),
                         index_col='timestamp',
                         parse_dates=True,
                         usecols=['timestamp', 'close'])
        return df_start.join(df)

    def get_dummy_portfolio(self, filepath):
        start = '2018-04-01'
        end = '2018-04-30'
        df = self.get_dummy_stock('SPY', pd.date_range(start, end)).dropna()
        df2 = pd.read_csv("tests/data/{}.csv".format(filepath),
                          index_col='timestamp',
                          parse_dates=True,
                          na_values=['nan'])
        df = df.join(df2).drop(columns='close')
        df.sort_index(ascending=True, inplace=True) # this is how Ryans datalake client does it
        return df

    def test_calculate_daily_returns(self):
        single_portfolio_item = "portfolio"
        utils = CalcUtils()
        portfolio = self.get_dummy_portfolio(single_portfolio_item)
        result = utils.get_daily_returns(portfolio)

        expected_trade_days = 21  # trade days in april 2018
        self.assertEqual(expected_trade_days, result.values.shape[0])

        # ensure shifted head was replaced with 0
        self.assertEqual(0, result.ix['2018-04-02'][0])

        # no returns for the first 3 days
        self.assertEqual(0, result.ix['2018-04-03'][0])
        self.assertEqual(0, result.ix['2018-04-04'][0])
        self.assertEqual(0, result.ix['2018-04-05'][0])

        # returns by 1 on the 6th and 16th
        self.assertEqual(1, result.ix['2018-04-06'][0])
        self.assertEqual(1, result.ix['2018-04-16'][0])

        # negative returns by half on the 30th
        self.assertEqual(-.5, result.ix['2018-04-30'][0])
        print(result)

    def test_calculate_daily_returns_multiple_values(self):
        many_portfolio_items = "portfolio_multiple"
        utils = CalcUtils()
        portfolio = self.get_dummy_portfolio(many_portfolio_items)
        result = utils.get_daily_returns(portfolio)

        # ensure shifted head was replaced with 0
        self.assertEqual(0, result.ix['2018-04-02'][0])

        expected_trade_days = 21  # trade days in april 2018
        self.assertEqual(expected_trade_days, result.values.shape[0])

        # no returns on the 2nd
        self.assertEqual(0, result.ix['2018-04-02'][0])
        self.assertEqual(0, result.ix['2018-04-02'][1])

        # returns for value1 but not value2 on the 6th
        self.assertEqual(1, result.ix['2018-04-06'][0])
        self.assertEqual(0, result.ix['2018-04-06'][1])

    def test_normalize(self):
        many_portfolio_items = "portfolio_multiple"
        utils = CalcUtils()
        portfolio = self.get_dummy_portfolio(many_portfolio_items)
        result = utils.normalize(portfolio)

        # normalized data should always start at 1
        self.assertEqual(1, result.ix['2018-04-02'][0])
        self.assertEqual(1, result.ix['2018-04-02'][1])

        # value1 column: .25 * 8 = 2, ie 8-fold growth
        self.assertEqual(8, result.ix['2018-04-30'][0])

        # value2 column: 0.5 *.5 = .25, i.e lost half its value
        self.assertEqual(0.5, result.ix['2018-04-30'][1])