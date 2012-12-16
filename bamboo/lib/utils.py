from math import isnan
import os
import re

from bamboo.config.settings import ASYNCHRONOUS_TASKS

# delimiter when passing multiple groups as a string
GROUP_DELIMITER = ','


def parse_int(value, default):
    try:
        return int(value)
    except ValueError:
        return default


def is_float_nan(num):
    """Return True is `num` is a float and NaN."""
    return isinstance(num, float) and isnan(num)


def slugify_columns(column_names):
    """Convert list of strings into unique slugs.

    Convert non-alphanumeric characters in column names into underscores and
    ensure that all column names are unique.

    :param column_names: A list of strings.

    :returns: A list slugified names with a one-to-one mapping to
        `column_names`.
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
    """Split a string based on the group delimiter"""
    return group_str.split(GROUP_DELIMITER)


def call_async(function, *args, **kwargs):
    """Potentially asynchronously call `function` with the arguments.

    :param function: The function to call.
    :param args: Arguments for the function.
    :param kwargs: Keyword arguments for the function.
    """
    if not os.getenv('BAMBOO_ASYNC_OFF') and ASYNCHRONOUS_TASKS:
        function.__getattribute__('apply_async')(args=args, kwargs=kwargs)
    else:  # pragma: no cover
        function(*args, **kwargs)
