from base64 import b64encode
from numpy import datetime64
import simplejson as json
import re

from bson import json_util


# _id is reserved by MongoDB
MONGO_RESERVED_KEY_PREFIX = '##'
MONGO_ID = '_id'
MONGO_ID_ENCODED = MONGO_RESERVED_KEY_PREFIX + '_id'

ILLEGAL_VALUES = ['$', '.']
REPLACEMENT_VALUES = [b64encode(value) for value in ILLEGAL_VALUES]

RE_ILLEGAL_MAP = [(re.compile(r'\%s' % value), REPLACEMENT_VALUES[idx]) for
                  idx, value in enumerate(ILLEGAL_VALUES)]
RE_LEGAL_MAP = [(re.compile(r'\%s' % value), ILLEGAL_VALUES[idx]) for
                idx, value in enumerate(REPLACEMENT_VALUES)]


def dump_mongo_json(obj):
    """Dump JSON using BSON conversion.

    Args:

    :param obj: Datastructure to dump as JSON.

    :returns: JSON string of dumped `obj`.
    """
    return json.dumps(obj, default=json_util.default)


def remove_mongo_reserved_keys(_dict):
    """Remove any keys reserved for MongoDB from `_dict`.

    Check for `MONGO_ID` in stored dictionary.  If found replace
    with unprefixed, if not found remove reserved key from dictionary.

    Args:

    :param _dict: Dictionary to remove reserved keys from.

    :returns: Dictionary with reserved keys removed.
    """
    if _dict.get(MONGO_ID_ENCODED):
        _dict[MONGO_ID] = _dict.pop(MONGO_ID_ENCODED)
    else:
        # remove mongo reserved keys
        del _dict[MONGO_ID]

    return _dict


def reserve_encoded(string):
    """Return encoding prefixed string."""
    return MONGO_ID_ENCODED if string == MONGO_ID else string


def dict_from_mongo(_dict):
    for key, value in _dict.items():
        if isinstance(value, list):
            value = [dict_from_mongo(obj)
                     if isinstance(obj, dict) else obj for obj in value]
        elif isinstance(value, dict):
            value = dict_from_mongo(value)

        if _was_encoded_for_mongo(key):
            del _dict[key]
            _dict[_decode_from_mongo(key)] = value

    return _dict


def dict_for_mongo(_dict):
    """Encode all keys in `_dict` for MongoDB."""
    for key, value in _dict.items():
        if _is_invalid_for_mongo(key):
            del _dict[key]
            key = key_for_mongo(key)

        if isinstance(value, list):
            _dict[key] = [dict_for_mongo(obj) if isinstance(obj, dict) else obj
                          for obj in value]
        elif isinstance(value, dict):
            _dict[key] = dict_for_mongo(value)
        else:
            _dict[key] = value_for_mongo(value)

    return _dict


def key_for_mongo(key):
    """Encode illegal MongoDB characters in string.

    Base64 encode any characters in a string that cannot be MongoDB keys. This
    includes any '$' and any '.'. '$' are supposed to be allowed as the
    non-first character but the current version of MongoDB does not allow any
    occurence of '$'.

    :param key: The string to remove characters from.

    :returns: The string with illegal keys encoded.
    """
    return reduce(lambda s, expr: expr[0].sub(expr[1], s),
                  RE_ILLEGAL_MAP, key)


def value_for_mongo(value):
    """Ensure value is a format acceptable for a MongoDB value.

    :param value: The value to encode.

    :returns: The encoded value.
    """
    if isinstance(value, datetime64):
        value = str(value)

    return value


def _decode_from_mongo(key):
    return reduce(lambda s, expr: expr[0].sub(expr[1], s),
                  RE_LEGAL_MAP, key)


def _is_invalid_for_mongo(key):
    """Return if string is invalid for storage in MongoDB."""
    return any([key.count(value) > 0 for value in ILLEGAL_VALUES])


def _was_encoded_for_mongo(key):
    return any([key.count(value) > 0 for value in REPLACEMENT_VALUES])
