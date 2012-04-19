import hashlib
from math import isnan

import numpy as np
from pandas import DataFrame

from constants import BAMBOO_ID, DEFAULT_HASH_ALGORITHM, JSON_NULL, MONGO_RESERVED_KEYS, MONGO_RESERVED_KEY_PREFIX


def is_float_nan(n):
    return isinstance(n, float) and isnan(n)


def get_json_value(v):
    if is_float_nan(v):
        return JSON_NULL
    if isinstance(v, np.int):
        return int(v)
    return v


def df_to_mongo(df):
    df = df_to_jsondict(df)
    for e in df:
        for k, v in e.items():
            if k in MONGO_RESERVED_KEYS:
                e[encode_key_for_mongo(k)] = v
                del e[k]
    return df


def mongo_to_df(m):
    return DataFrame(mongo_decode_keys(m))


def mongo_decode_keys(rows):
    for row in rows:
        del row[BAMBOO_ID]
        for k, v in row.items():
            if k in MONGO_RESERVED_KEYS:
                v = row.pop(encode_key_for_mongo(k))
                if v != 'null':
                    row[k] = v
                else:
                    del row[k]
    return rows


def encode_key_for_mongo(k):
    return '%s%s' % (MONGO_RESERVED_KEY_PREFIX, k)


def series_to_jsondict(s):
    return s if s is None else dict([(k, get_json_value(v)) for k, v in s.iteritems()])


def df_to_jsondict(df):
    return [series_to_jsondict(s) for i, s in df.iterrows()]


def df_to_hexdigest(df, algo=DEFAULT_HASH_ALGORITHM):
    h = hashlib.new(algo)
    h.update(df.to_string())
    return h.hexdigest()
