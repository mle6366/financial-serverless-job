"""
 Expanse, LLC
 http://expansellc.io

 Copyright 2018
 Released under the Apache 2 license
 https://www.apache.org/licenses/LICENSE-2.0

 @authors Meghan Erickson
"""
import logging
import botocore
from io import BytesIO
import os

BUCKET = os.environ['BUCKET']
KEY = os.environ['KEY']
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PortfolioClient:
    def __init__(self, s3):
        self.s3 = s3
        self.exists = True
        try:
            self.s3.head_bucket(Bucket=BUCKET)
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logging.error("PortfolioClient: "
                              "The {} bucket could not be found. ".format(BUCKET))
                self.exists = False

    def send_to_bucket(self, df):
        if self.exists:
            logging.info('PortfolioClient : attempting to send portfolio to '
                         'destination {} {}'.format(BUCKET, KEY))
            df_buffer = BytesIO()
            df_buffer.write(b"timestamp,total\n")
            df_buffer.write(bytes(df.to_csv(), 'utf-8'))
            df_buffer.seek(0)
            try:
                self.s3.upload_fileobj(df_buffer, BUCKET, KEY)
                logging.info('PortfolioClient : sent portfolio to '
                             'destination {} {}'.format(BUCKET, KEY))
            except botocore.exceptions.ClientError as e:
                msg = e.response.get('Error', {}).get('Message', 'Could Not Retrieve Error Message')
                logging.error("PortfolioClient BotocoreClientException sending portfolio "
                              "to destination {} {}: {}"
                              .format(BUCKET, KEY, msg))
            except Exception as e:
                logging.error('PortfolioClient Exception end portfolio to'
                              'destination {} {}: {}'
                              .format(BUCKET, KEY, e))
        else:
            logging.error('PortfolioClient : failed to send portfolio to'
                          'destination {} {}'.format(BUCKET, KEY))

    def get_portfolio_from_bucket(self):
        """
            Gets the existing portfolio from the bucket.
            :return stream of bytes
        """
        if self.exists:
            logging.info('PortfolioClient : attempting to get portfolio from '
                         'src {} {}'.format(BUCKET, KEY))
            result = self.s3.get_object(Bucket=BUCKET, Key=KEY)
            return result['Body'].read()
        else:
            logging.error('PortfolioClient : failed to get portfolio from'
                          ' src {} {}'.format(BUCKET, KEY))
