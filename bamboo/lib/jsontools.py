import numpy as np

from bamboo.lib.utils import is_float_nan


# JSON encoding string
JSON_NULL = 'null'


class JSONError(Exception):
    """For errors while parsing JSON."""
    pass


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
