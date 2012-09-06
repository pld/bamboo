from datetime import datetime
import json
from math import isnan
import re
from calendar import timegm

from dateutil.parser import parse as date_parse
import numpy as np

from constants import ERROR, JSON_NULL, LABEL, MONGO_RESERVED_KEYS,\
    MONGO_RESERVED_KEY_PREFIX


def is_float_nan(num):
    return isinstance(num, float) and isnan(num)


def get_json_value(value):
    if is_float_nan(value):
        return JSON_NULL
    if isinstance(value, np.int64):
        return int(value)
    return value


def series_to_jsondict(series):
    return series if series is None else dict([
        (str(key), get_json_value(value))
        for key, value in series.iteritems()
    ])


def df_to_jsondict(dframe):
    return [series_to_jsondict(series) for idx, series in dframe.iterrows()]


def dump_or_error(data, error_message):
    if data is None:
        data = {ERROR: error_message}
    return json.dumps(data)


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
                # attempt to parse first entry as a date
                date_parse(dframe[column][0])
                # it is parseable as a date, convert column to date
                dframe[column] = dframe[column].map(date_parse)
            except ValueError:
                # it is not a correctly formatted date
                pass
    return dframe


def type_for_data_and_dtypes(type_map, column, dtype_type):
    field_type = type(column[0])
    return type_map[field_type if field_type == datetime else dtype_type]


def parse_str_to_unix_time(value):
    return parse_date_to_unix_time(date_parse(value))


def parse_date_to_unix_time(date):
    return timegm(date.utctimetuple())


def reserve_encoded(string):
    return prefix_reserved_key(string) if string in MONGO_RESERVED_KEYS else\
        string
