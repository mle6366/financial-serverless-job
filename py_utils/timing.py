"""
 Expanse, LLC
 http://expansellc.io

 Copyright 2018
 Released under the Apache 2 license
 https://www.apache.org/licenses/LICENSE-2.0

 @authors Ryan Scott
"""
import logging
from datetime import datetime
from functools import wraps


def timing(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = f(*args, **kwargs)
        end = datetime.now()
        logging.debug('[PERF] {} elapsed time: {}ms'.format(f.__name__, int((end - start).total_seconds() * 1000)))
        return result

    return wrapper
