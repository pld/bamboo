import json
import numpy as np
import re

from bson import json_util
from pandas import DataFrame

from lib.constants import DATASET_OBSERVATION_ID, DEFAULT_HASH_ALGORITHM,\
         ENCODED_DOLLAR, ENCODED_DOT, ENCODED_KEY_REGEX, MONGO_RESERVED_KEYS,\
         MONGO_RESERVED_KEY_PREFIX
from lib.utils import df_to_jsondict, get_json_value


def df_to_mongo(dframe):
    """
    Prefix mongos reserved keys and ensure keys are in mongo acceptable format.
    """
    return [
        series if series is None else _dict_for_mongo(dict(
            [
                (_prefix_mongo_reserved_key(key) if key in MONGO_RESERVED_KEYS else
                        key, get_json_value(value))
                for key, value in series.iteritems()
            ]))
        for idx, series in dframe.iterrows()
    ]


def _dict_for_mongo(d):
    """
    Ensure keys do not begin with '$' or contain a '.'
    """
    for key, value in d.items():
        if _is_invalid_for_mongo(key):
            del d[key]
            d[_encode_for_mongo(key)] = value
    return d


def _is_invalid_for_mongo(key):
    return key.startswith('$') or key.count('.') > 0


def _is_encoded_key(key):
    return True if ENCODED_KEY_REGEX.search(key) else False


def dict_from_mongo(_dict):
    """
    Return the dictionary *_dict* (which may have other internal data structures)
    with any keys decoded if they had been previously encoded for mongo.
    """
    for key, value in _dict.iteritems():
        if _is_encoded_key(key):
            del _dict[key]
            key = _decode_from_mongo(key)
        elif type(value) == dict:
            value = dict_from_mongo(value)
        _dict[key] = value
    return _dict


def _encode_for_mongo(key):
    """
    Base64 encode keys that begin with a '$' or contain a '.'
    """
    return reduce(lambda s, c: re.sub(c[0], c[1], s),
            [(r'^\$', ENCODED_DOLLAR), (r'\.', ENCODED_DOT)], key)


def _decode_from_mongo(key):
    """
    Decode Base64 keys that contained a '$' or '.'
    """
    return reduce(lambda s, c: re.sub(c[0], c[1], s),
            [(r'^%s' % ENCODED_DOLLAR, '$'), (r'%s' % ENCODED_DOT, '.')], key)


def _prefix_mongo_reserved_key(key):
    """
    Prefix mongo reserved key
    """
    return '%s%s' % (MONGO_RESERVED_KEY_PREFIX, key)


def mongo_to_df(cursor):
    return DataFrame(mongo_decode_keys([row for row in cursor]))


def mongo_to_json(cursor):
    """
    Convert mongo *cursor* to json dict, via dataframe, then dump to JSON.
    """
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
        observation.pop(DATASET_OBSERVATION_ID, None)
        observation = mongo_remove_reserved_keys(observation)

    return observations

def mongo_remove_reserved_keys(_dict):
    for key, value in _dict.items():
        if key in MONGO_RESERVED_KEYS:
            prefixed_key = _prefix_mongo_reserved_key(key)
            if _dict.get(prefixed_key):
                # replace reserved key value with original key value
                value = _dict.pop(prefixed_key)
                _dict[key] = value
            else:
                # remove mongo reserved keys
                del _dict[key]
        elif value == 'null':
            _dict[key] = np.nan
    return _dict
