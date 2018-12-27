import logging
import time

import pandas as pd

from py_utils.datalake import DataLake
from py_utils.dataframe_util import DataframeUtil


class Portfolio:
    def __init__(self, datalake_client=None):
        if datalake_client is None:
            datalake_client = DataLake()
        self.datalake_client = datalake_client
        self.holdings = None
        self.validation = DataframeUtil()

    def get_date_range(self, start_date, end_date):
        """
        Date range to index and scope the portfolio.

        :param (string) start_date like '2017-12-11'

        :param (string) end_date like '2017-12-11'

        :return (pandas.DateTimeIndex)
        """
        date_range = pd.date_range(start_date, end_date)
        return date_range

    def build_portfolio(self, date_time_index):
        """
        Creates a series representing MegRyans portfolio cumulative  (total) value,
            indexed by DateTimeIndex.

        :param date_time_index: (pandas.DatetimeIndex) for bounding timeseries

        :return series, indexed by date, each row as daily total portfolio value.
        """
        self.holdings = self.__get_holdings(date_time_index)
        symbols = list(self.holdings.columns.values)
        logging.info('{} || Portfolio || Preparing to build portfolio from the following '
                     'symbols: {}'
                     .format(time.asctime(time.localtime(time.time())), symbols))
        cols = ['timestamp', 'close']
        df = self.get_starter_dataframe(date_time_index, cols)
        df = df.rename(columns={'close': 'SPY'})
        for symbol in symbols:
            if symbol == 'timestamp' or symbol == 'SPY':
                continue
            logging.info('{} || Portfolio || '
                         'Searching for Stock {}'
                         .format(time.asctime(time.localtime(time.time())), symbol))
            df_tmp = self.datalake_client.get_stock(symbol, date_time_index, cols)
            if self.validation.is_invalid_dataframe(df_tmp, symbol):
                logging.error('{} Dataframe is empty or otherwise invalid'.format(symbol))
                raise RuntimeError('{} || Portfolio || Encountered invalid dataframe {} '
                                   'while building portfolio. Short-circuiting.'
                                   .format(time.asctime(time.localtime(time.time())), symbol))
            df_tmp = df_tmp.rename(columns={'close': symbol})
            df = df.join(df_tmp)
        df.drop_duplicates(inplace=True)
        holdings_vals = self.calculate_position_values(df, date_time_index)
        portfolio_val = self.__calculate_total_portfolio_val(holdings_vals)
        return portfolio_val

    def get_starter_dataframe(self, date_time_index, cols=None):
        """
        Creates dataframe with DateTimeIndex as index,
            constrained by S&P_500 trade days.
        The coming portfolio dataframe will be left-joined to this,
            so that the portfolio can omit non-trade days.

        :param date_time_index: (pandas.DatetimeIndex) for bounding timeseries

        :param columns: (list of string) columns to include in dataframe, must include index

        :return dataframe
        """
        df = pd.DataFrame(index=date_time_index)
        df_spy = self.datalake_client.get_stock('SPY', date_time_index, cols)
        return df.join(df_spy).dropna()

    def calculate_position_values(self, df, date_time_index):
        """
        Calculates every stock's daily dollar position in the portfolio,
            based off holdings and the stock price on that date.

        :param df: (pandas.DataFrame) representing portfolio from PersonalCapital

        :param date_time_index: (pandas.DatetimeIndex) date range we want to see

        :return: (pandas.DataFrame) indexed by date, each column as a stock, each row
            represents that stock's total value in the portfolio on that date.
        """
        df = df.sort_index(ascending=False).drop(columns='SPY')  # we dont have SPY holdings
        holdings = self.holdings if self.holdings is not None \
            else self.__get_holdings(date_time_index)
        result = df.mul(holdings)
        return result

    def __calculate_total_portfolio_val(self, df):
        """
        Takes as input the resulting dataframe from calculate_position_values(),
            and returns that dataframe's summation

        :param df: (pandas.DataFrame) the resulting df from calculate_position_values()

        :return: (series) Indexed by date, each row is the day's total portfolio value
        """
        result = df.sum(axis=1)
        return result

    def __calculate_allocation_percentages(self, df):
        """
        Calculates allocations as a percentage of the total portfolio value.
            This method is currently unused and requires tlc.

        :param df: dataframe

        :return: dataframe of each holding (stock) and its associated percentage
        """
        df = df.sort_index(ascending=False).head(1)
        date_range = self.get_date_range(df.index[0], df.index[0])
        holdings_df = self.holdings if self.holdings is not None \
            else self.__get_holdings(date_range)
        test = df.mul(holdings_df)
        total_value = test.ix[0].sum()
        return test.div(total_value, axis=1)

    def __get_holdings(self, date_time_index):
        df = self.datalake_client.get_holdings(date_time_index)
        if self.validation.is_invalid_dataframe(df):
            raise RuntimeError('{} || Portfolio || Encountered invalid dataframe'
                               'while building portfolio - unable to get holdings.'
                               ' Short-circuiting.'
                               .format(time.asctime(time.localtime(time.time()))))
        start_df = self.get_starter_dataframe(date_time_index, cols=['timestamp', 'close'])
        return start_df.join(df).drop(columns=['close'])
