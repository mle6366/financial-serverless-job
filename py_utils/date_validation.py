"""
 Expanse, LLC
 http://expansellc.io

 Copyright 2018
 Released under the Apache 2 license
 https://www.apache.org/licenses/LICENSE-2.0

 @authors Meghan Erickson
"""
import datetime
from py_utils.rest_utils.client_bad_request import ClientBadRequest
import logging


class DateValidation:

    def validate_dates(self, start=None, end=None):
        fmt = '%Y-%m-%d'
        message = "Bad Request. Must provide valid start and end date."
        if start is None and end is None:
            return

        try:
            start = datetime.datetime.strptime(start, fmt)
            end = datetime.datetime.strptime(end, fmt)
        except Exception as e:
            logging.error('Portfolio Service Exception: {}'.format(str(e)))
            raise ClientBadRequest(message, status=400, payload=str(e))

        if start > end:
            raise ClientBadRequest(message, status=400,
                                   payload='Start date must be before end date.')