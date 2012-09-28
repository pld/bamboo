import json
import numpy as np
import re

from bson import json_util
from pandas import DataFrame

from lib.constants import BAMBOO_RESERVED_KEYS, MONGO_RESERVED_KEYS
from lib.utils import df_to_jsondict, get_json_value, prefix_reserved_key


def mongo_to_df(rows):
    """
    Decode all row keys and create DataFrame.
    """
    return DataFrame(mongo_decode_keys(rows))


def mongo_to_json(rows):
    """
    Convert mongo *cursor* to json dict, via dataframe, then dump to JSON.
    """
    jsondict = df_to_jsondict(mongo_to_df(rows))
    return dump_mongo_json(jsondict)


def dump_mongo_json(_dict):
    return json.dumps(_dict, default=json_util.default)


def mongo_decode_keys(observations):
    """
    Remove internal keys from collection records.
    Decode keys that were encoded for mongo.
    """
    for observation in observations:
        observation = remove_bamboo_reserved_keys(observation)
        observation = remove_mongo_reserved_keys(observation)

    return observations


def remove_bamboo_reserved_keys(_dict):
    for reserved_key in BAMBOO_RESERVED_KEYS:
        _dict.pop(reserved_key, None)
    return _dict


def remove_mongo_reserved_keys(_dict):
    """
    Check for *MONGO_RESERVED_KEYS* in stored dictionary.  If found replace
    with unprefixed, if not found remove reserved key from dictionary.
    """
    for key in MONGO_RESERVED_KEYS:
        prefixed_key = prefix_reserved_key(key)
        if _dict.get(prefixed_key):
            _dict[key] = _dict.pop(prefixed_key)
        else:
            # remove mongo reserved keys
            del _dict[key]
    return _dict
