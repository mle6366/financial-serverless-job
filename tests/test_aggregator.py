import unittest
from unittest.mock import MagicMock

from aggregator import Aggregator

class TestAggregator(unittest.TestCase):

    def setUp(self):
        pass

    def test_starter_dataframe(self):
        agg = Aggregator()
        s = '2017-12-11'
        e = '2018-04-29'
        cols = ['timestamp', 'open', 'close']
        date_range = agg.get_date_range(s, e)
        df = agg.get_starter_dataframe(date_range, cols)
        expected_headers = ['open', 'close']
        self.assertEqual(expected_headers, list(df.columns.values))