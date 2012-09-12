from datetime import datetime
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
MODE_INFO = 'info'
MODE_RELATED = 'related'
MODE_SUMMARY = 'summary'

# common MongoDB keys
BAMBOO_RESERVED_KEY_PREFIX = 'BAMBOO_RESERVED_KEY_'
DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_id'
LINKED_DATASETS = BAMBOO_RESERVED_KEY_PREFIX + 'linked_datasets'
DATASET_OBSERVATION_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_observation_id'

# special MongoDB keys
MONGO_RESERVED_KEYS = ['_id']
MONGO_RESERVED_KEY_PREFIX = 'MONGO_RESERVED_KEY'
MONGO_RESERVED_KEY_STRS = [MONGO_RESERVED_KEY_PREFIX + key
                           for key in MONGO_RESERVED_KEYS]

# internal caching keys
ALL = '_all'
STATS = '_stats'

# JSON output labels
ERROR = 'error'
ID = 'id'
SUCCESS = 'success'

# metadata
ATTRIBUTION = 'attribution'
CREATED_AT = 'created_at'
DESCRIPTION = 'description'
LABEL = 'label'
LICENSE = 'license'
NUM_COLUMNS = 'num_columns'
NUM_ROWS = 'num_rows'
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
DATETIME = 'datetime'

# olap_types
DIMENSION = 'dimension'
MEASURE = 'measure'

# map from numpy objects to simpletypes
DTYPE_TO_SIMPLETYPE_MAP = {
    np.bool_:   BOOLEAN,
    np.float64: FLOAT,
    np.int64:   INTEGER,
    np.object_: STRING,
    datetime: DATETIME,
}

# map from numpy objects to olap_types
DTYPE_TO_OLAP_TYPE_MAP = {
    np.object_: DIMENSION,
    np.bool_: DIMENSION,
    np.float64: MEASURE,
    np.int64: MEASURE,
    datetime: MEASURE,
}

# batch size for bulk inserts
DB_BATCH_SIZE = 1000
