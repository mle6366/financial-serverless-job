import pandas as pd
import numpy as np
import time


class Aggregator:

    def get_date_range(self, start_date, end_date):
        """Define and Return the Pandas DateTimeIndex"""
        date_range = pd.date_range(start_date, end_date)
        return date_range

    def build_portfolio(self, date_time_index):
        'TODO Call Ryans Client'
        symbols = list(self.get_holdings(date_time_index).columns.values)
        cols = ['timestamp', 'close']
        df = self.get_starter_dataframe(date_time_index, cols)
        df = df.rename(columns={'close': 'SPY'})
        for symbol in symbols:
            if symbol == 'timestamp' or symbol == 'SPY':
                continue
            df_tmp = self.get_stock_data(symbol, cols)
            df_tmp = df_tmp.rename(columns={'close': symbol})
            df = df.join(df_tmp)
        self.calculate_allocations(df)
        return df

    def get_starter_dataframe(self, date_time_index, cols):
        """Returns a dataframe constrained by S&P500 trade days,
            thus omitting holidays/weekends"""
        df = pd.DataFrame(index=date_time_index)
        df_spy = self.get_stock_data('SPY', cols)
        return df.join(df_spy).dropna()

    def normalize_portfolio(self, df):
        """divide each value in the df by day 1 (row 0)"""
        return df/df.ix[0]

    def calculate_allocations(self, df):
        """To Revisit: Calculate based off Cash as well, not just investments"""
        df = df.sort_index(ascending=False).head(1)
        date_range = self.get_date_range(df.index[0], df.index[0])
        holdings_df = self.get_holdings(date_range)
        test = df.mul(holdings_df)
        total_value = test.ix[0].sum()
        allocs = test.div(total_value, axis=1)
        print('did this add to: {}'.format(allocs.ix[0].sum()))

    def get_holdings(self, date_time_index=None):
        df = pd.DataFrame(index=date_time_index)
        df2 = pd.read_csv("tests/data/holdings.csv",
                           index_col='timestamp',
                           parse_dates=True,
                           na_values=['nan'])
        df = df.join(df2)
        df.sort_index(ascending=False, inplace=True)
        return df

    def get_stock_data(self, stock, columns):
        df_temp = pd.read_csv("tests/data/{}.csv".format(stock),
                              index_col='timestamp',
                              parse_dates=True,
                              na_values=['nan'],
                              usecols=columns)
        return df_temp
