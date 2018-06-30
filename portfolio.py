import pandas as pd
import aggregator as agg

if __name__ == "__main__":
    print("Hello World.")
    s = '2017-12-11'
    e = '2018-04-29'
    portfolio = agg.build_portfolio(
        agg.get_date_range(s, e))
    print("Stop Here")
