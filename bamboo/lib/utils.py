from itertools import chain
from math import isnan
from sys import maxint

import numpy as np


def flatten(list_):
    return [item for sublist in list_ for item in sublist]


def combine_dicts(*dicts):
    """Combine dicts with keys in later dicts taking precedence."""
    return dict(chain(*[_dict.iteritems() for _dict in dicts]))


def invert_dict(dict_):
    return {v: k for (k, v) in dict_.items()} if dict_ else {}


def is_float_nan(num):
    """Return True is `num` is a float and NaN."""
    return isinstance(num, float) and isnan(num)


def minint():
    return -maxint - 1


def parse_float(value, default=None):
    return _parse_type(np.float64, value, default)


def parse_int(value, default=None):
    return _parse_type(int, value, default)


def _parse_type(_type, value, default):
    try:
        return _type(value)
    except ValueError:
        return default


def replace_keys(original, mapping):
    """Recursively replace any keys in original with their values in mappnig.

    :param original: The dictionary to replace keys in.
    :param mapping: A dict mapping keys to new keys.

    :returns: Original with keys replaced via mapping.
    """
    return original if not type(original) in (dict, list) else {
        mapping.get(k, k): {
            dict: lambda: replace_keys(v, mapping),
            list: lambda: [replace_keys(vi, mapping) for vi in v]
        }.get(type(v), lambda: v)() for k, v in original.iteritems()}


def to_list(maybe_list):
    return maybe_list if isinstance(maybe_list, list) else [maybe_list]
