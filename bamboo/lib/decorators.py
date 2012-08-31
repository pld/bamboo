from functools import wraps
import time
import urllib2


class classproperty(property):

    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()


def print_time(func):
    """
    @print_time

    Put this decorator around a function to see how many seconds each
    call of this function takes to run.
    """
    def wrapped_func(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        seconds = end - start
        print "SECONDS:", seconds, func.__name__, kwargs
        return result
    return wrapped_func


def requires_internet(f, url='http://74.125.113.99'):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not _internet_on(url):
            raise AssertionError('This test requires an internet connection to'
                                 'run/pass.')
        return f(*args, **kwargs)
    return wrapper


def _check_url(url, timeout=1):
    try:
        response = urllib2.urlopen(url, timeout=timeout)
        return True
    except urllib2.URLError as e:
        pass
    return False


def _internet_on(url):
    return _check_url(url)
