import re

import numpy as np


# versioning
VERSION_NUMBER = '0.1'
VERSION_DESCRIPTION = 'alpha'

# hash algorithm parameter
DEFAULT_HASH_ALGORITHM = 'sha1'

# JSON encoding string
JSON_NULL = 'null'

# common MongoDB keys
BAMBOO_RESERVED_KEY_PREFIX = 'BAMBOO_RESERVED_KEY_'
DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_id'
DATASET_OBSERVATION_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_observation_id'

# special MongoDB keys
MONGO_RESERVED_KEYS = ['_id']
MONGO_RESERVED_KEY_PREFIX = 'MONGO_RESERVED_KEY'

# internal caching keys
ALL = '_all'
STATS = '_stats'

# JSON output labels
ERROR = 'error'
ID = 'id'
SUCCESS = 'success'

# metadata
CREATED_AT = 'created_at'
SCHEMA = 'schema'
SIMPLETYPE = 'simpletype'
SUMMARY = 'summary'
UPDATED_AT = 'updated_at'

# simpletypes
STRING = 'string'
INTEGER = 'integer'
FLOAT = 'float'
BOOLEAN = 'boolean'

# map from numpy objects to simpletypes
DTYPE_TO_SIMPLETYPE_MAP = {
    np.object_: STRING,
    np.int64:   INTEGER,
    np.float64: FLOAT,
    np.bool_:   BOOLEAN,
}

# batch size for bulk inserts
DB_BATCH_SIZE = 1000

# regex for finding encoded mongo keys
ENCODED_DOLLAR = 'JA=='
ENCODED_DOT = 'Lg=='
ENCODED_KEY_REGEX = re.compile(r'%s|%s' % (ENCODED_DOLLAR, ENCODED_DOT))
