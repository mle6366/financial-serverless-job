import pandas as pd


class CalcUtils:

    def normalize(self, df):
        """
        Divides each value in the dataframe by its corresponding row 0 (day 1) value.
        Used to normalize price data so all prices start at 1.0 at the beginning of time.
        :param df: (pandas.DataFrame) the beginning dataframe
        :return: (pandas.DataFrame) the normalized dataframe
        """
        df.sort_index(ascending=True, inplace=True)
        return df / df.ix[0]

    def get_daily_returns(self, df):
        """
        For each day (row) we calculate daily return.
            Formula = (today / yesterday) - 1
        For accuracy in this calculation, we verify most recent records are
            towards the tail of the dataframe:
                df.sort_index(ascending=True)
        :param df: (pandas.DatetimeIndex).
        :return: df (pandas.DatetimeIndex) of daily return values
        """
        df.sort_index(ascending=True, inplace=True)
        daily_returns = (df / df.shift(1)) - 1
        daily_returns.ix[0] = 0
        return daily_returns