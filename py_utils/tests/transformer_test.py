import pandas as pd
import unittest
import datetime
import json
import logging
from transformer import Transformer


class TransformUtilTest(unittest.TestCase):

    def setUp(self):
        self.single_column_csv = "tests/data/transformer/portfolio.csv"
        self.multi_column_csv = "tests/data/transformer/multi-column.csv"

    def test_transformSingleColumn(self):
        # a single column in a csv (such as our portfolio)
        #   will evaluate to a single series, like: { points[ {x,y} ]}
        expected = {
            'points': [{
                'x': [1531440000000000000, 1531699200000000000, 1531785600000000000],
                'y': [586530.845803, 598698.1160159999, 601614.5574609999]
            }]
        }
        transformer = Transformer()
        df = self.__prepareDataFrameFromCsv(self.single_column_csv)
        result = json.loads(transformer.plotly_tranform(df))
        print(json.dumps(result, indent=2))

        self.assertTrue('points' in expected)
        self.assertTrue(isinstance(result['points'], list))
        self.assertEqual(1, len(result['points']))

        self.assertTrue('x' in result['points'][0])
        self.assertTrue('y' in result['points'][0])
        self.assertTrue(isinstance(result['points'][0]['y'], list))
        self.assertTrue(isinstance(result['points'][0]['x'], list))

        self.assertEqual(3, len(expected['points'][0]['x']))
        self.assertEqual(3, len(expected['points'][0]['y']))

        # date ('x') must be sequential, oldest date on top
        self.assertEqual(expected['points'][0]['x'][0], result['points'][0]['x'][0])
        self.assertEqual(expected['points'][0]['x'][1], result['points'][0]['x'][1])
        self.assertEqual(expected['points'][0]['x'][2], result['points'][0]['x'][2])
        # total ('y') must be sequential
        self.assertEqual(expected['points'][0]['y'][0], result['points'][0]['y'][0])
        self.assertEqual(expected['points'][0]['y'][1], result['points'][0]['y'][1])
        self.assertEqual(expected['points'][0]['y'][2], result['points'][0]['y'][2])

    def test_transformMultipleColumns(self):
        # multiple columns in a csv (for example representing multiple stocks change over time)
        #    will evaluate to multiple series, like: { points[ {x,y}, {x,y} . . . n ] }
        expected = {
            'points': [
                {
                    'x': [1531440000000000000, 1531699200000000000, 1531785600000000000],
                    'y': [2.0, 0.0, 1.5]
                },
                {
                    'x': [1531440000000000000, 1531699200000000000, 1531785600000000000],
                    'y': [0.25, 0.5, 0.75]
                }
            ]
        }
        transformer = Transformer()
        df = self.__prepareDataFrameFromCsv(self.multi_column_csv)
        result = json.loads(transformer.plotly_tranform(df))
        print(json.dumps(result))

        self.assertTrue('points' in expected)
        self.assertTrue(isinstance(result['points'], list))
        # two columns means 2 points
        self.assertEqual(2, len(result['points']))
        self.assertTrue(isinstance(result['points'][0]['x'], list))
        self.assertTrue(isinstance(result['points'][0]['y'], list))
        self.assertTrue(isinstance(result['points'][1]['x'], list))
        self.assertTrue(isinstance(result['points'][1]['y'], list))

        self.assertEqual(3, len(expected['points'][0]['x']))
        self.assertEqual(expected['points'][0]['x'][0], result['points'][0]['x'][0])
        self.assertEqual(expected['points'][0]['x'][1], result['points'][0]['x'][1])
        self.assertEqual(expected['points'][0]['x'][2], result['points'][0]['x'][2])

        self.assertEqual(3, len(expected['points'][1]['x']))
        self.assertEqual(expected['points'][1]['x'][0], result['points'][1]['x'][0])
        self.assertEqual(expected['points'][1]['x'][1], result['points'][1]['x'][1])
        self.assertEqual(expected['points'][1]['x'][2], result['points'][1]['x'][2])

        self.assertEqual(3, len(expected['points'][0]['y']))
        self.assertEqual(expected['points'][0]['y'][0], result['points'][0]['y'][0])
        self.assertEqual(expected['points'][0]['y'][1], result['points'][0]['y'][1])
        self.assertEqual(expected['points'][0]['y'][2], result['points'][0]['y'][2])

        self.assertEqual(3, len(expected['points'][1]['y']))
        self.assertEqual(expected['points'][1]['y'][0], result['points'][1]['y'][0])
        self.assertEqual(expected['points'][1]['y'][1], result['points'][1]['y'][1])
        self.assertEqual(expected['points'][1]['y'][2], result['points'][1]['y'][2])

    def __prepareDataFrameFromCsv(self, filepath):
        '''
         :param filepath: String representing file,
            ex tests/data/portfolio.csv
        :return df: Pandas Dataframe
         '''
        try:
            df = pd.read_csv(filepath,
                             index_col='timestamp',
                             parse_dates=True)
        except Exception as e:
            logging.error('Error; Could not read csv from '
                          'filepath {}. Exception message is {}'
                          .format(filepath, e))
            now = datetime.datetime.now()
            start = "{0:%Y-%m-%d}".format(now - datetime.timedelta(7, 0))
            end = "{0:%Y-%m-%d}".format(now)
            df = pd.DataFrame(index=pd.date_range(start, end))
        return df
