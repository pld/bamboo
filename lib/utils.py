import json
from math import isnan
import re

import numpy as np

from constants import ERROR, JSON_NULL


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


def dump_or_error(data, error_message):
    return json.dumps(data or {ERROR: error_message})


def slugify_columns(column_names):
    """
    Convert non-alphanumeric characters in column names into underscores and
    ensure that all column names are unique.
    """
    encode_column_re = re.compile(r'\W')

    encoded_names = []

    for column_name in column_names:
        new_col_name = encode_column_re.sub('_', column_name).lower()
        while new_col_name in encoded_names:
            new_col_name += '_'
        encoded_names.append(new_col_name)
    return encoded_names
