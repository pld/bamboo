"""
This stores constants used in multiple files.
"""

# reserved bamboo keys
BAMBOO_RESERVED_KEY_PREFIX = 'BAMBOO_RESERVED_KEY_'
# TODO remove bamboo reserved key prefix from dataset_id,
# when we can migrate live db
DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_id'
DATASET_OBSERVATION_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_observation_id'
PARENT_DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'parent_dataset_id'
BAMBOO_RESERVED_KEYS = [
    DATASET_OBSERVATION_ID,
    PARENT_DATASET_ID,
]

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
