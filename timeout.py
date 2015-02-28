from functools import wraps
import errno
import os
from threading import Timer

import logging

class TimeoutError(Exception):
    pass

def timeout(seconds=30, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(*args,**kwargs):
            logging.warn("timedout")
            raise TimeoutError(error_message)

        @wraps(func)
        def wrapper(*args, **kwargs):
            t = Timer( seconds, _handle_timeout )
            t.start()
            try:
                result = func(*args, **kwargs)
            finally:
                t.cancel()
            return result

        wrapper.func_name = func.func_name
        return wrapper
        
    return decorator
