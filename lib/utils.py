import json
import hashlib
from math import isnan
import re
import urllib2

import numpy as np
from bson import json_util
from pandas import DataFrame

from constants import DATASET_ID, DEFAULT_HASH_ALGORITHM, JSON_NULL,\
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
    df = df_to_jsondict(df)
    for e in df:
        for k, v in e.items():
            if k in MONGO_RESERVED_KEYS:
                e[encode_key_for_mongo(k)] = v
                del e[k]
    return df


def mongo_to_df(cursor):
    return DataFrame(mongo_decode_keys([x for x in cursor]))


def mongo_to_json(cursor):
    jsondict = df_to_jsondict(mongo_to_df(cursor))
    return json.dumps(jsondict, default=json_util.default)


def mongo_decode_keys(observations):
    """
    Remove internal keys from collection records.
    Decode keys that were encoded for mongo.
    """
    for observation in observations:
        del observation[DATASET_ID]
        for key, value in observation.items():
            if key in MONGO_RESERVED_KEYS and observation.get(encode_key_for_mongo(key)):
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


def df_to_hexdigest(df, algo=DEFAULT_HASH_ALGORITHM):
    h = hashlib.new(algo)
    h.update(df.to_string())
    return h.hexdigest()

def open_data_file(url):
    open_url = lambda d: urllib2.urlopen(d['url'])
    protocols = {
        'http':  open_url,
        'https': open_url,
        'file':  lambda d: d['path'],
    }
    regex = re.compile(
        '^(?P<url>(?P<protocol>%s):\/\/(?P<path>.+))$' \
        % '|'.join(protocols.keys())
    )
    match = re.match(regex, url)
    if match:
        args = match.groupdict()
        return protocols[args['protocol']](args)
    return None


class classproperty(property):

    def __get__(self, cls, owner):
        return self.fget.__get__(None, owner)()
