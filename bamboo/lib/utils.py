from math import isnan
import os
import re

from pandas import Series

from bamboo.config.settings import ASYNCHRONOUS_TASKS

"""
Constants for utils
"""

# delimiter when passing multiple groups as a string
GROUP_DELIMITER = ','


def is_float_nan(num):
    return isinstance(num, float) and isnan(num)


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


def call_async(function, *args, **kwargs):
    if not os.getenv('BAMBOO_ASYNC_OFF') and ASYNCHRONOUS_TASKS:
        function.__getattribute__('apply_async')(args=args, kwargs=kwargs)
    else:  # pragma: no cover
        function(*args, **kwargs)
