import re

"""
This stores constants used in multiple files.
"""

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

# JSON output labels
ERROR = 'error'
ID = 'id'

# metadata
NUM_COLUMNS = 'num_columns'
NUM_ROWS = 'num_rows'
SCHEMA = 'schema'
SIMPLETYPE = 'simpletype'
SUMMARY = 'summary'

# datetime simpletype
DATETIME = 'datetime'

# olap_types
DIMENSION = 'dimension'
MEASURE = 'measure'

# batch size for bulk inserts
DB_BATCH_SIZE = 1000
