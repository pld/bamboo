import re


# hash algorithm parameter
DEFAULT_HASH_ALGORITHM = 'sha1'

# JSON encoding string
JSON_NULL = 'null'

# common MongoDB keys
BAMBOO_RESERVED_KEY_PREFIX = 'BAMBOO_RESERVED_KEY_'
DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_id'
DATASET_OBSERVATION_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_observation_id'
FORMULA = 'formula'
STATS = '_stats'

# special MongoDB keys
MONGO_RESERVED_KEYS = ['_id']
MONGO_RESERVED_KEY_PREFIX = 'MONGO_RESERVED_KEY'

# output labels
ALL = '_all'
DATA = 'data'
NAME = 'name'
SUMMARY = 'summary'

# batch size for bulk inserts
DB_BATCH_SIZE = 1000

# regex for finding encoded mongo keys
ENCODED_DOLLAR = 'JA=='
ENCODED_DOT = 'Lg=='
ENCODED_KEY_REGEX = re.compile(r'%s|%s' % (ENCODED_DOLLAR, ENCODED_DOT))
