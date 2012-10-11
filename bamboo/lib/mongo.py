import json
import numpy as np
import re

from bson import json_util

from bamboo.lib.jsontools import get_json_value


# MongoDB keys
MONGO_RESERVED_KEYS = ['_id']
MONGO_RESERVED_KEY_PREFIX = 'MONGO_RESERVED_KEY'
MONGO_RESERVED_KEY_STRS = [MONGO_RESERVED_KEY_PREFIX + key
                           for key in MONGO_RESERVED_KEYS]


def mongo_to_df(rows):
    """
    Decode all row keys and create DataFrame.
    """
    return mongo_decode_keys(rows)


def dump_mongo_json(_dict):
    return json.dumps(_dict, default=json_util.default)


def mongo_decode_keys(observations):
    """
    Remove internal keys from collection records.
    Decode keys that were encoded for mongo.
    """
    return [
        remove_mongo_reserved_keys(observation) for observation in
        observations
    ]


def remove_mongo_reserved_keys(_dict):
    """
    Check for *MONGO_RESERVED_KEYS* in stored dictionary.  If found replace
    with unprefixed, if not found remove reserved key from dictionary.
    """
    for key in MONGO_RESERVED_KEYS:
        prefixed_key = mongo_prefix_reserved_key(key)
        if _dict.get(prefixed_key):
            _dict[key] = _dict.pop(prefixed_key)
        else:
            # remove mongo reserved keys
            del _dict[key]
    return _dict


def mongo_prefix_reserved_key(key, prefix=MONGO_RESERVED_KEY_PREFIX):
    """
    Prefix reserved key
    """
    return '%s%s' % (prefix, key)


def reserve_encoded(string):
    return mongo_prefix_reserved_key(string) if\
        string in MONGO_RESERVED_KEYS else string
