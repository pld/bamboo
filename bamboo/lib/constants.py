"""
This stores constants used in multiple files.
"""

# reserved bamboo keys
BAMBOO_RESERVED_KEY_PREFIX = 'BAMBOO_RESERVED_KEY_'
DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_id'
DATASET_OBSERVATION_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_observation_id'
PARENT_DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'parent_dataset_id'
BAMBOO_RESERVED_KEYS = [
    DATASET_ID,
    DATASET_OBSERVATION_ID,
    PARENT_DATASET_ID,
]

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
