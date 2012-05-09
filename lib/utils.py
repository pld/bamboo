import json
import hashlib
from math import isnan

import numpy as np
from bson import json_util
from pandas import DataFrame

from constants import DATASET_OBSERVATION_ID, DEFAULT_HASH_ALGORITHM, JSON_NULL,\
         MONGO_RESERVED_KEYS, MONGO_RESERVED_KEY_PREFIX


def is_float_nan(n):
    return isinstance(n, float) and isnan(n)


def get_json_value(v):
    if is_float_nan(v):
        return JSON_NULL
    if isinstance(v, np.int):
        return int(v)
    return v


def df_to_mongo(df):
    return [
        series if series is None else dict(
            [
                (encode_key_for_mongo(key) if key in MONGO_RESERVED_KEYS else
                        key, get_json_value(value))
                for key, value in series.iteritems()
            ])
        for idx, series in df.iterrows()
    ]


def mongo_to_df(cursor):
    return DataFrame(mongo_decode_keys([x for x in cursor]))


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


def encode_key_for_mongo(k):
    return '%s%s' % (MONGO_RESERVED_KEY_PREFIX, k)


def series_to_jsondict(s):
    return s if s is None else dict([(k, get_json_value(v)) for k, v in s.iteritems()])


def df_to_jsondict(df):
    return [series_to_jsondict(s) for i, s in df.iterrows()]
