from math import isnan

from pandas import DataFrame


MONGO_RESERVED_KEYS = ['_id']
MONGO_RESERVED_KEY_PREFIX = 'MONGO_RESERVED_KEY_'
JSON_NULL = 'null'


def is_float_nan(n):
    return isinstance(n, float) and isnan(n)


def get_json_value(v):
    if is_float_nan(v):
        return JSON_NULL
    return v


def df_to_mongo(df):
    df = df_to_json(df)
    for e in df:
        for k, v in e.items():
            if k in MONGO_RESERVED_KEYS:
                e[encode_key_for_mongo(k)] = v
                del e[k]
    return df


def mongo_to_df(m):
    return DataFrame(mongo_to_json(m))


def mongo_to_json(m):
    for e in m:
        for k, v in e.items():
            if k in MONGO_RESERVED_KEYS:
                e[k] = e.pop(encode_key_for_mongo(k))
    return m


def encode_key_for_mongo(k):
    return '%s%s' % (MONGO_RESERVED_KEY_PREFIX, k)


def series_to_json(s):
    return dict([(k, get_json_value(v)) for k, v in s.iteritems()])


def df_to_json(df):
    return [series_to_json(s) for i, s in df.iterrows()]
