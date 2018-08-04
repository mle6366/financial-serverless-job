import logging
import botocore
from io import BytesIO

BUCKET = "expansellc-datalake"
KEY = 'portfolio/portfolio.csv'
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
                logging.error("The {} bucket could not be found. ".format(BUCKET))
                self.exists = False

    def send_to_bucket(self, df):
        if self.exists:
            logging.info("PortfolioClient : attempting to send portfolio to "
                         "destination expansellc-datalake/portfolio/portfolio.csv.")
            df_buffer = BytesIO()
            df_buffer.write(b"timestamp,total\n")
            df_buffer.write(bytes(df.to_csv(), 'utf-8'))
            df_buffer.seek(0)
            self.s3.upload_fileobj(df_buffer, BUCKET, KEY)
        else:
            logging.error("PortfolioClient : failed to send portfolio to"
                          "destination expansellc-datalake/portfolio/portfolio.csv.")
