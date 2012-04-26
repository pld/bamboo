from functools import wraps
import urllib2


def requires_internet(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not _internet_on():
            raise AssertionError('This test requires an internet connection to run/pass.')
        return f(*args, **kwargs)
    return wrapper


def _check_url(url, timeout=1):
    try:
        response = urllib2.urlopen(url, timeout=timeout)
        return True
    except urllib2.URLError as e:
        pass
    return False


def _internet_on(url='http://74.125.113.99'):
    return _check_url(url)
