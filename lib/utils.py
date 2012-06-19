import json
from math import isnan
import re

import numpy as np

from constants import ERROR, JSON_NULL, LABEL, MONGO_RESERVED_KEYS,\
         MONGO_RESERVED_KEY_PREFIX, SCHEMA


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


def prefix_reserved_key(key):
    """
    Prefix reserved key
    """
    return '%s%s' % (MONGO_RESERVED_KEY_PREFIX, key)


def slugify_columns(column_names):
    """
    Convert non-alphanumeric characters in column names into underscores and
    ensure that all column names are unique.
    """
    # we need to change this to deal with the following conditions:
    # * _id as a key (mongo)
    # * keys that start with a $ or contain a . (mongo)
    # * keys that contain spaces or operators (parsing)
    encode_column_re = re.compile(r'\W')

    encoded_names = []

    for column_name in column_names:
        if column_name in MONGO_RESERVED_KEYS:
            column_name = prefix_reserved_key(column_name)
        new_col_name = encode_column_re.sub('_', column_name).lower()
        while new_col_name in encoded_names:
            new_col_name += '_'
        encoded_names.append(new_col_name)
    return encoded_names


def build_labels_to_slugs(dataset):
    """
    Map the column labels back to their slugified versions
    """
    return dict([(column_attrs[LABEL], column_name) for
            (column_name, column_attrs) in dataset[SCHEMA].items()])
