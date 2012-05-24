from base64 import b64encode
import json
import re

from bson import json_util
from pandas import DataFrame

from lib.constants import DATASET_OBSERVATION_ID, DEFAULT_HASH_ALGORITHM,\
         MONGO_RESERVED_KEYS, MONGO_RESERVED_KEY_PREFIX
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


def _encode_for_mongo(key):
    """
    Base64 encode keys that begin with a '$' or contain a '.'
    """
    return reduce(lambda s, c: re.sub(c[0], b64encode(c[1]), s),
            [(r'^\$', '$'), (r'\.', '.')], key)


def _prefix_mongo_reserved_key(key):
    """
    Prefix mongo reserved key
    """
    return '%s%s' % (MONGO_RESERVED_KEY_PREFIX, key)


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
                    _prefix_mongo_reserved_key(key)):
                value = observation.pop(_prefix_mongo_reserved_key(key))
                if value != 'null':
                    observation[key] = value
                else:
                    del observation[key]
    return observations
