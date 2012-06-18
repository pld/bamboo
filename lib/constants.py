import re

import numpy as np


# versioning
VERSION_NUMBER = '0.1'
VERSION_DESCRIPTION = 'alpha'

# hash algorithm parameter
DEFAULT_HASH_ALGORITHM = 'sha1'

# JSON encoding string
JSON_NULL = 'null'

# modes for dataset controller
MODE_SUMMARY = 'summary'
MODE_INFO = 'info'

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
LABEL = 'label'
OLAP_TYPE = 'olap_type'
SCHEMA = 'schema'
SIMPLETYPE = 'simpletype'
SUMMARY = 'summary'
UPDATED_AT = 'updated_at'

# simpletypes
STRING = 'string'
INTEGER = 'integer'
FLOAT = 'float'
BOOLEAN = 'boolean'

# olap_types
DIMENSION = 'dimension'
MEASURE = 'measure'

# map from numpy objects to simpletypes
DTYPE_TO_SIMPLETYPE_MAP = {
    np.bool_:   BOOLEAN,
    np.float64: FLOAT,
    np.int64:   INTEGER,
    np.object_: STRING,
}

# map from numpy objects to olap_types
DTYPE_TO_OLAP_TYPE_MAP = {
    np.object_: DIMENSION,
    np.bool_: DIMENSION,
    np.float64: MEASURE,
    np.int64: MEASURE,
}

# batch size for bulk inserts
DB_BATCH_SIZE = 1000

# regex for finding encoded mongo keys
ENCODED_DOLLAR = 'JA=='
ENCODED_DOT = 'Lg=='
ENCODED_KEY_REGEX = re.compile(r'%s|%s' % (ENCODED_DOLLAR, ENCODED_DOT))
