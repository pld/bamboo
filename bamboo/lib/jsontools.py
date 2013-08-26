from bson import json_util
import numpy as np
import simplejson as json

from bamboo.lib.mongo import dump_mongo_json
from bamboo.lib.utils import is_float_nan


# JSON encoding string
JSON_NULL = 'null'


class JSONError(Exception):
    """For errors while parsing JSON."""
    pass


def df_to_jsondict(df):
    """Return DataFrame as a list of dicts for each row."""
    return [series_to_jsondict(series) for _, series in df.iterrows()]


def df_to_json(df):
    """Convert DataFrame to a list of dicts, then dump to JSON."""
    jsondict = df_to_jsondict(df)
    return dump_mongo_json(jsondict)


def get_json_value(value):
    """Parse JSON value based on type."""
    if is_float_nan(value):
        value = JSON_NULL
    elif isinstance(value, np.int64):
        value = int(value)
    elif isinstance(value, np.bool_):
        value = bool(value)

    return value


def series_to_jsondict(series):
    """Convert a Series to a dictionary encodable as JSON."""
    return series if series is None else {
        unicode(key): get_json_value(value)
        for key, value in series.iteritems()
    }


def safe_json_loads(string, error_title='string'):
    try:
        return string and json.loads(string, object_hook=json_util.object_hook)
    except ValueError as err:
        raise JSONError('cannot decode %s: %s' % (error_title, err.__str__()))
