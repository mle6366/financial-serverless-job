import pandas as pd
import numpy as np
import time


def get_date_range(start_date, end_date):
    """Define and Return the Pandas DateTimeIndex"""
    date_range = pd.date_range(start_date, end_date)
    return date_range


def build_portfolio(date_time_index):
    """
    In the future this should build off PersonalCapital for accuracy,
    but for now this will use pre-defined manual entries,
    since our holdings don't change too often.

    For now we'll manually adjust quarterly since that's when the most
    portfolio movement occurs

    """

    'TODO Call Ryans Client'
    holdings = get_holdings()
    stocks = list(holdings.columns.values)
    df = pd.DataFrame(index=date_time_index)
    cols = ['timestamp', 'close']
    for stock in stocks:
        if stock == 'timestamp':
            continue
        df_tmp = get_stock_data(stock, cols)
        df_tmp = df_tmp.rename(columns={'close': stock})
        df = df.join(df_tmp)
    calculate_allocations(df)
    return df


def normalize_portfolio(df):
    """divide the dataframe by it's first row, ie divide each columns val by day1 val"""
    return df/df.ix[0]


def calculate_allocations(df):
    """To Revisit: Calculate based off Cash as well, not just investments"""
    # total portfolio value as of today (or latest day)
    # shares total value per stock as of today (or latest)
    # alloc = %age of shares value against total portfolio value
    stocks = list(df.columns.values)
    # print(df)
    last_t_day = get_last_trading_day(df)
    print("Last Trade Day is {}".format(last_t_day))


def get_last_trading_day(df):
    """Compare to S&P 500"""
    df = df['SPY']
    df.sort_index(ascending=False, inplace=True)
    latest = df.head(4).dropna().head().index[0]
    return latest


def get_holdings():
    """Return List of Tuples Like: [{stock:alloc},{'NVDA':100}]"""
    return pd.read_csv("tests/data/holdings.csv")


def get_stock_data(stock, columns):
    df_temp = pd.read_csv("tests/data/{}.csv".format(stock),
                          index_col='timestamp',
                          parse_dates=True,
                          na_values=['nan'],
                          usecols=columns)
    return df_temp
