from math import isnan
from sys import maxint


def minint():
    return -maxint - 1


def parse_int(value, default):
    return _parse_type(int, value, default)


def parse_float(value, default=None):
    return _parse_type(float, value, default)


def _parse_type(_type, value, default):
    try:
        return _type(value)
    except ValueError:
        return value if default is None else default


def is_float_nan(num):
    """Return True is `num` is a float and NaN."""
    return isinstance(num, float) and isnan(num)
