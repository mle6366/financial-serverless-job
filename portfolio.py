from aggregator import Aggregator

if __name__ == "__main__":
    agg = Aggregator()
    s = '2017-04-01'
    e = '2018-04-29'
    portfolio = agg.build_portfolio(
        agg.get_date_range(s, e))
