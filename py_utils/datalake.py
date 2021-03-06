"""
 Expanse, LLC
 http://expansellc.io

 Copyright 2018
 Released under the Apache 2 license
 https://www.apache.org/licenses/LICENSE-2.0

 @authors Ryan Scott
"""
import datetime
import logging
import os
import boto3
import pandas as pd

DATALAKE = "expansellc-datalake"


class DataLake:
    """
    Provides methods for interacting with the files of the DataLake.
    """

    def __init__(self, client=None):
        """
        Creates an instance of DataLake.

        :param date_range: (DatetimeIndex) for bounding data

        :param client: boto3 client (only used in testing)
        """
        # If we are no provided a client, create a default
        if client is None:
            client = boto3.resource("s3", region_name="us-west-2")
        self.client = client

        # if date_range is None:
        now = datetime.datetime.now()
        start = "{0:%Y-%m-%d}".format(now - datetime.timedelta(30, 0))
        end = "{0:%Y-%m-%d}".format(now)
        self.date_range = pd.date_range(start, end)

    def _get_datalake_file(self, path):
        """
        Downloads a specific file from the DataLake based on the path.

        :param path: (str) path to file in the DataLake

        :return: (str) path to the downloaded file
        """
        output = "/tmp/{}.datalake".format(path.replace("/", "_"))

        if os.path.isfile(output):
            logging.debug(
                "existing temp file '{}' was found, removing".format(output))
            os.remove(output)

        try:
            self.client.Bucket(DATALAKE).download_file(path, output)
        except Exception as err:
            logging.error("encountered error with s3, {}".format(err))
            return None

        return output

    def get_stock_raw(self, symbol):
        """
        Retrieves the data for a specific stock from the DataLake in raw file form.

       :param symbol: (string) stock symbol

        :return: path to csv file
        """
        path = "assets/{}/data.csv".format(symbol.upper())
        return self._get_datalake_file(path=path)

    def get_stock(self, symbol, date_range=None, columns=None):
        """
        Retrieves the data for a specific stock from the DataLake as a DataFrame.

        :param date_range: (Pandas DateTimeIndex) date_range

        :param symbol: (string) stock symbol

        :param columns: (list of string) columns to include in dataframe, must include index

        :return: DataFrame
        """
        output = self.get_stock_raw(symbol=symbol)
        if output is None:
            return self.__create_empty_df(date_range)

        if columns is not None:
            df = pd.read_csv(output,
                             index_col="timestamp",
                             parse_dates=True,
                             na_values=["nan"],
                             usecols=columns)
        else:
            df = pd.read_csv(output,
                             index_col="timestamp",
                             parse_dates=True,
                             na_values=["nan"])

        result_frame = self.__create_empty_df(date_range)
        return result_frame.join(df)

    def __create_empty_df(self, date_range=None):
        return pd.DataFrame(index=date_range if date_range is not None else self.date_range)

    def get_holding(self, symbol, date_range=None):
        """
        Retrieves the holding data for a specific stock from the DataLake as a DataFrame.

        :param symbol: (string) stock symbol

        :return: DataFrame
        """
        path = "personalcapital/holdings/{}.csv".format(symbol)
        tmp = self._get_datalake_file(path=path)
        df = pd.read_csv(tmp,
                         index_col="timestamp",
                         parse_dates=True,
                         na_values=["nan"])
        result_frame = pd.DataFrame(
            index=date_range if date_range is not None else self.date_range)
        return result_frame.join(df)

    def get_holdings(self, date_range=None, ascending=False):
        """
        Retrieves all holding data from the DataLake and joins as a single DataFrame.

        :return: DataFrame
        """
        result_frame = pd.DataFrame(
            index=self.date_range if date_range is None else date_range
        )

        objs = self.client.Bucket(DATALAKE).objects.filter(
            Prefix="personalcapital/holdings/")
        for obj in list(objs):
            # Simplify the s3 object path to just the stock symbol
            simple_key = obj.key.replace("personalcapital/holdings/", "")
            idx = simple_key.index(".csv")
            symbol = simple_key[:idx]

            logging.debug("getting holding for '{}'".format(symbol))
            df = self.get_holding(symbol=symbol, date_range=date_range)

            result_frame = result_frame.join(df)

        result_frame.sort_index(ascending=ascending, inplace=True)
        return result_frame
