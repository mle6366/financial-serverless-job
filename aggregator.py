import pandas as pd

import logging
import time
from datalake import DataLake


class Aggregator:
    def __init__(self, datalake_client=None):
        if datalake_client is None:
            datalake_client = DataLake()
        self.datalake_client = datalake_client
        self.holdings = None

    def get_date_range(self, start_date, end_date):
        """Define and Return the Pandas DateTimeIndex"""
        date_range = pd.date_range(start_date, end_date)
        return date_range

    def build_portfolio(self, date_time_index):
        self.holdings = self.get_holdings(date_time_index)
        symbols = list(self.holdings.columns.values)
        cols = ['timestamp', 'close']
        df = self.get_starter_dataframe(date_time_index, cols)
        df = df.rename(columns={'close': 'SPY'})
        for symbol in symbols:
            if symbol == 'timestamp' or symbol == 'SPY':
                continue
            logging.info("{} || Aggregator || "
                         "Searching for Stock {}"
                         .format(time.asctime(time.localtime(time.time())), symbol))
            df_tmp = self.datalake_client.get_stock(symbol, date_time_index, cols)
            if self.is_invalid_dataframe(df_tmp, symbol):
                continue
            df_tmp = df_tmp.rename(columns={'close': symbol})
            df = df.join(df_tmp)
        holdings_vals = self.calculate_position_values(df, date_time_index)
        portfolio_val = self.calculate_total_portfolio_val(holdings_vals)
        print(portfolio_val)
        logging.info(portfolio_val.values)
        logging.info(holdings_vals)
        return portfolio_val

    def get_starter_dataframe(self, date_time_index, cols=None):
        """
        Returns a dataframe constrained by S&P500 trade days,
        thus omitting holidays/weekends.

        :param date_time_index: (DatetimeIndex) for bounding timeseries

        :param columns: (list of string) columns to include in dataframe, must include index

        """
        df = pd.DataFrame(index=date_time_index)
        df_spy = self.datalake_client.get_stock('SPY', date_time_index, cols)
        return df.join(df_spy).dropna()

    def normalize(self, df):
        """divide each value in the df by day 1 (row 0)"""
        return df/df.ix[0]

    def calculate_position_values(self, df, date_time_index):
        """ TODO: Use the date_time_index from the incoming dataframe"""
        df = df.sort_index(ascending=False).drop(columns='SPY') # we dont have SPY holdings
        holdings = self.holdings if self.holdings is not None else self.get_holdings(date_time_index)
        result = df.mul(holdings)
        return result

    def calculate_allocation_percentages(self, df):
        """TODO: Calculate based off Cash as well, not just investments"""
        df = df.sort_index(ascending=False).head(1)
        date_range = self.get_date_range(df.index[0], df.index[0])
        holdings_df = self.holdings if self.holdings is not None else self.get_holdings(date_range)
        test = df.mul(holdings_df)
        total_value = test.ix[0].sum()
        allocs = test.div(total_value, axis=1)
        print('did this add to: {}'.format(allocs.ix[0].sum()))

    def calculate_total_portfolio_val(self, df):
        return df.sum(axis=1)

    def get_holdings(self, date_time_index):
        df = self.datalake_client.get_holdings(date_time_index)
        if self.is_invalid_dataframe(df):
            return None
        start_df = self.get_starter_dataframe(date_time_index, cols=['timestamp', 'close'])
        return start_df.join(df).drop(columns=['close'])

    def is_invalid_dataframe(self, df, symbol=None):
        if df.empty:
            logging.error("{} : Aggregator.py:Cannot load Holdings for {}. "
                          .format(time.asctime(time.localtime(time.time())),
                                  symbol if symbol is not None else "No Symbol Info"))

        return df.empty