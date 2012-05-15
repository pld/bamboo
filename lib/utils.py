import json
import hashlib
from math import isnan

import numpy as np
from bson import json_util
from pandas import DataFrame

from constants import DATASET_OBSERVATION_ID, DEFAULT_HASH_ALGORITHM,\
         JSON_NULL, MONGO_RESERVED_KEYS, MONGO_RESERVED_KEY_PREFIX


def is_float_nan(num):
    return isinstance(num, float) and isnan(num)


def get_json_value(value):
    if is_float_nan(value):
        return JSON_NULL
    if isinstance(value, np.int):
        return int(value)
    return value


def df_to_mongo(dframe):
    return [
        series if series is None else dict(
            [
                (encode_key_for_mongo(key) if key in MONGO_RESERVED_KEYS else
                        key, get_json_value(value))
                for key, value in series.iteritems()
            ])
        for idx, series in dframe.iterrows()
    ]


def mongo_to_df(cursor):
    return DataFrame(mongo_decode_keys([row for row in cursor]))


def mongo_to_json(cursor):
    jsondict = df_to_jsondict(mongo_to_df(cursor))
    return dump_mongo_json(jsondict)


def dump_mongo_json(_dict):
    return json.dumps(_dict, default=json_util.default)


def mongo_decode_keys(observations):
    """
    Remove internal keys from collection records.
    Decode keys that were encoded for mongo.
    """
    for observation in observations:
        del observation[DATASET_OBSERVATION_ID]
        for key, value in observation.items():
            if key in MONGO_RESERVED_KEYS and observation.get(
                    encode_key_for_mongo(key)):
                value = observation.pop(encode_key_for_mongo(key))
                if value != 'null':
                    observation[key] = value
                else:
                    del observation[key]
    return observations


def encode_key_for_mongo(key):
    return '%s%s' % (MONGO_RESERVED_KEY_PREFIX, key)


def series_to_jsondict(series):
    return series if series is None else dict([
        (key, get_json_value(value))
        for key, value in series.iteritems()
    ])


def df_to_jsondict(dframe):
    return [series_to_jsondict(series) for idx, series in dframe.iterrows()]
