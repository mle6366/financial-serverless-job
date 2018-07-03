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
