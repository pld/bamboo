from math import isnan
import os

from bamboo.config.settings import is_async

# delimiter when passing multiple groups as a string
GROUP_DELIMITER = ','


def parse_int(value, default):
    return _parse_type(int, value, default)


def parse_float(value, default):
    return _parse_type(float, value, default)


def _parse_type(_type, value, default):
    try:
        return _type(value)
    except ValueError:
        return default


def is_float_nan(num):
    """Return True is `num` is a float and NaN."""
    return isinstance(num, float) and isnan(num)


def split_groups(group_str):
    """Split a string based on the group delimiter"""
    return group_str.split(GROUP_DELIMITER)


def call_async(function, *args, **kwargs):
    """Potentially asynchronously call `function` with the arguments.

    :param function: The function to call.
    :param args: Arguments for the function.
    :param kwargs: Keyword arguments for the function.
    """
    if is_async():
        function.__getattribute__('apply_async')(args=args, kwargs=kwargs)
    else:  # pragma: no cover
        function(*args, **kwargs)
