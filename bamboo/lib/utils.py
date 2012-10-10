from math import isnan
import re

import numpy as np
from pandas import Series

from bamboo.config.settings import ASYNCHRONOUS_TASKS
from bamboo.lib.constants import PARENT_DATASET_ID

"""
Constants for utils
"""

# JSON encoding string
JSON_NULL = 'null'

# delimiter when passing multiple groups as a string
GROUP_DELIMITER = ','


def is_float_nan(num):
    return isinstance(num, float) and isnan(num)


def get_json_value(value):
    if is_float_nan(value):
        value = JSON_NULL
    elif isinstance(value, np.int64):
        value = int(value)
    elif isinstance(value, np.bool_):
        value = bool(value)
    return value


def series_to_jsondict(series):
    return series if series is None else dict([
        (unicode(key), get_json_value(value))
        for key, value in series.iteritems()
    ])


def df_to_jsondict(dframe):
    return [series_to_jsondict(series) for idx, series in dframe.iterrows()]


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


def split_groups(group_str):
    return group_str.split(GROUP_DELIMITER)


def call_async(function, dataset, *args, **kwargs):
    if ASYNCHRONOUS_TASKS:
        function.__getattribute__('apply_async')(
            args=args, kwargs=kwargs, queue=dataset.dataset_id)
    else:  # pragma: no cover
        function(*args, **kwargs)


def add_parent_column(parent_dataset_id, dframe):
    column = Series([parent_dataset_id] * len(dframe))
    column.name = PARENT_DATASET_ID
    return dframe.join(column)
