import pandas as pd

import time
import logging


class ValidationUtils:

    def is_invalid_dataframe(self, df, symbol=None):
        """
        Validation if data_lake client returns empty dataframe.
        :param df: (pandas.DataFrame)
        :param symbol: (String) requested symbol
        :return: boolean
        """
        if df.empty:
            logging.error("{} : Cannot load Holdings for {}. "
                          .format(time.asctime(time.localtime(time.time())),
                                  symbol if symbol is not None else "No Symbol Info"))

        return df.empty