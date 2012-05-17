from math import isnan

import numpy as np

from constants import JSON_NULL


def is_float_nan(num):
    return isinstance(num, float) and isnan(num)


def get_json_value(value):
    if is_float_nan(value):
        return JSON_NULL
    if isinstance(value, np.int):
        return int(value)
    return value


def series_to_jsondict(series):
    return series if series is None else dict([
        (key, get_json_value(value))
        for key, value in series.iteritems()
    ])


def df_to_jsondict(dframe):
    return [series_to_jsondict(series) for idx, series in dframe.iterrows()]
