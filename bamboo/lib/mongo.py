from  base64 import b64encode
import simplejson as json
import re

from bson import json_util


# MongoDB keys
MONGO_RESERVED_KEYS = ['_id']
MONGO_RESERVED_KEY_PREFIX = 'MONGO_RESERVED_KEY'
MONGO_RESERVED_KEY_STRS = [MONGO_RESERVED_KEY_PREFIX + key
                           for key in MONGO_RESERVED_KEYS]
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

    Check for `MONGO_RESERVED_KEYS` in stored dictionary.  If found replace
    with unprefixed, if not found remove reserved key from dictionary.

    Args:

    :param _dict: Dictionary to remove reserved keys from.

    :returns: Dictionary with reserved keys removed.
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
    """Prefix reserved key."""
    return '%s%s' % (prefix, key)


def reserve_encoded(string):
    """Return encoding prefixed string."""
    return mongo_prefix_reserved_key(string) if\
        string in MONGO_RESERVED_KEYS else string


def dict_from_mongo(_dict):
    for key, value in _dict.items():
        if isinstance(value, list):
            value = [dict_from_mongo(obj)
                     if isinstance(obj, dict) else obj for obj in value]
        elif isinstance(value, dict):
            value = dict_from_mongo(value)
        elif _was_encoded_for_mongo(key):
            del _dict[key]
            _dict[_decode_from_mongo(key)] = value

    return _dict


def _decode_from_mongo(key):
    return reduce(lambda s, expr: expr[0].sub(expr[1], s),
                  RE_LEGAL_MAP, key)


def dict_for_mongo(_dict):
    """Encode all keys in `_dict` for MongoDB."""
    for key, value in _dict.items():
        if isinstance(value, list):
            value = [dict_for_mongo(obj)
                     if isinstance(obj, dict) else obj for obj in value]
        elif isinstance(value, dict):
            value = dict_for_mongo(value)
        elif _is_invalid_for_mongo(key):
            del _dict[key]
            _dict[_encode_for_mongo(key)] = value

    return _dict


def _encode_for_mongo(key):
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


def _is_invalid_for_mongo(key):
    """Return if string is invalid for storage in MongoDB."""
    return any([key.count(value) > 0 for value in ILLEGAL_VALUES])


def _was_encoded_for_mongo(key):
    return any([key.count(value) > 0 for value in REPLACEMENT_VALUES])
