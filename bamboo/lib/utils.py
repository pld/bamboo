import simplejson as json
from math import isnan
import re
from calendar import timegm

from dateutil.parser import parse as date_parse
import numpy as np
from pandas import Series

from constants import DATETIME, ERROR, MONGO_RESERVED_KEYS,\
    MONGO_RESERVED_KEY_PREFIX, SIMPLETYPE
from config.settings import ASYNCHRONOUS_TASKS

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


def dump_or_error(data, error_message):
    if data is None:
        data = {ERROR: error_message}
    return json.dumps(data)


def prefix_reserved_key(key, prefix=MONGO_RESERVED_KEY_PREFIX):
    """
    Prefix reserved key
    """
    return '%s%s' % (prefix, key)


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
        new_col_name = encode_column_re.sub('_', column_name).lower()
        while new_col_name in encoded_names:
            new_col_name += '_'
        encoded_names.append(new_col_name)
    return encoded_names


def recognize_dates(dframe):
    """
    Check if object columns in a dataframe can be parsed as dates.
    If yes, rewrite column with values parsed as dates.
    """
    for idx, dtype in enumerate(dframe.dtypes):
        if dtype.type == np.object_:
            try:
                column = dframe.columns[idx]
                new_column = Series([
                    field if is_float_nan(field) else date_parse(field) for
                    field in dframe[column].tolist()])
                dframe[column] = new_column
            except ValueError:
                # it is not a correctly formatted date
                pass
            except OverflowError:
                # it is a number that is too large to be a date
                pass
    return dframe


def recognize_dates_from_schema(dataset, dframe):
    # if it is a date column, recognize dates
    for column, column_schema in dataset.data_schema.items():
        if dframe.get(column) and column_schema[SIMPLETYPE] == DATETIME:
            dframe[column] = dframe[column].map(date_parse)
    return dframe


def parse_str_to_unix_time(value):
    return parse_date_to_unix_time(date_parse(value))


def parse_date_to_unix_time(date):
    return timegm(date.utctimetuple())


def reserve_encoded(string):
    return prefix_reserved_key(string) if string in MONGO_RESERVED_KEYS else\
        string


def split_groups(group_str):
    return group_str.split(GROUP_DELIMITER)


def call_async(function, *args, **kwargs):
    if ASYNCHRONOUS_TASKS:
        function.__getattribute__('delay')(*args, **kwargs)
    else:
        function(*args, **kwargs)
