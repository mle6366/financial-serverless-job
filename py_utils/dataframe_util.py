"""
 Expanse, LLC
 http://expansellc.io

 Copyright 2018
 Released under the Apache 2 license
 https://www.apache.org/licenses/LICENSE-2.0

 @authors Meghan Erickson
"""
import pandas as pd
import numpy as np
from io import StringIO
import logging
import datetime
import time


class DataframeUtil:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    def __init__(self):
        now = datetime.datetime.now()
        start = "{0:%Y-%m-%d}".format(now - datetime.timedelta(7, 0))
        end = "{0:%Y-%m-%d}".format(now)
        self.date_range = pd.date_range(start, end)

    def handle_csv_bytestream(self, csv_bytestream, datatype=np.float64):
        """
        This will tranform the s3 csv_bytestream response
        into a valid dataframe,
        or return an empty dataframe if the csv is malformed.

        :param csv_bytestream: bystream response from Boto S3
        :param datatype: numpy datatype, like np.float32
        :return: pandas Dataframe
        """
        s = str(csv_bytestream, 'utf-8')

        # allows for buffered reading of the String
        buffered_string = StringIO(s)

        try:
            df = pd.read_csv(buffered_string,
                             dtype=datatype,
                             index_col='timestamp',
                             parse_dates=True)
        except Exception as e:
            logging.error('DataframeUtil: Error parsing s3 csv into dataframe. '
                          ' {}'.format(e))
            df = pd.DataFrame(index=self.date_range)
        return df

    def is_invalid_dataframe(self, df, symbol=None):
        """
        Logs if data_lake client returns empty dataframe.
        :param df: (pandas.DataFrame)
        :param symbol: (String) requested symbol
        :return: boolean
        """
        if df.empty:
            logging.error('{} : Cannot load Holdings for {}. '
                          .format(time.asctime(time.localtime(time.time())),
                                  symbol if symbol is not None else 'No Symbol Info'))
        return df.empty