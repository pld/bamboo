from math import isnan

from pandas import DataFrame


MONGO_RESERVED_KEYS = ['_id']
MONGO_RESERVED_KEY_PREFIX = 'MONGO_RESERVED_KEY_'


def is_float_nan(n):
    return isinstance(n, float) and isnan(n)


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


def df_to_json(df):
    return [
        dict([
            (colname, 'null') if is_float_nan(row[i]) else (colname, row[i])
            for i, colname in enumerate(df.columns)
        ])
        for row in df.values
    ]
