import json
import numpy as np
import re

from bson import json_util
from pandas import DataFrame

from lib.constants import DATASET_OBSERVATION_ID, DEFAULT_HASH_ALGORITHM,\
    MONGO_RESERVED_KEYS
from lib.utils import df_to_jsondict, get_json_value, prefix_reserved_key


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
    """
    Check for *MONGO_RESERVED_KEYS* in stored dictionary.  If found replaced
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
