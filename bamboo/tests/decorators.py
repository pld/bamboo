from functools import wraps
import os
import time
import urllib2

from bamboo.config.settings import RUN_PROFILER


def requires_async(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        del os.environ['BAMBOO_ASYNC_OFF']
        self = args[0]
        result = func(*args, **kwargs)
        os.environ['BAMBOO_ASYNC_OFF'] = 'True'
        return result
    return wrapper


def run_profiler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if RUN_PROFILER:
            return func(*args, **kwargs)
    return wrapper


def print_time(func):
    """
    @print_time

    Put this decorator around a function to see how many seconds each
    call of this function takes to run.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        seconds = end - start
        print "SECONDS:", seconds, func.__name__, kwargs
        return result
    return wrapper
