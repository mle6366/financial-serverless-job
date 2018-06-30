import logging

import boto3
import pandas as pd

log = logging.getLogger()
log.setLevel(level=logging.INFO)
DATALAKE = "expansellc-datalake"


class DataLake:
    """
    Provides methods for interacting with the files of the DataLake.
    """

    def __init__(self, client=None):
        # If we are no provided a client, create a default
        if client is None:
            client = boto3.resource("s3", region_name="us-west-2")

        self.client = client
        pass

    def get_stock_raw(self, symbol):
        """
        Retrieves the data for a specific stock from the DataLake in raw file form.

        :param symbol: (string) stock symbol

        :return: path to csv file
        """
        path = "alphavantage/{}/data.csv".format(symbol.upper())
        output = "/tmp/{}.csv".format(path.replace("/", "_"))

        try:
            self.client.Bucket(DATALAKE).download_file(path, output)
        except Exception as err:
            log.error("encountered error with s3, %s".format(err))
            return None

        return output

    def get_stock(self, symbol, columns=None):
        """
        Retrieves the data for a specific stock from the DataLake as a DataFrame.

        :param symbol: (string) stock symbol

        :param columns: (list of string) columns to include in dataframe, must include index

        :return: DataFrame
        """
        output = self.get_stock_raw(symbol=symbol)
        if output is None:
            return output

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
        return df
