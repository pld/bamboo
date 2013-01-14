import os
import sys

from bamboo.lib.async import is_async, set_async


# database config
ASYNC_FLAG = 'BAMBOO_ASYNC_OFF'
DATABASE_NAME = 'bamboo_dev'
TEST_DATABASE_NAME = DATABASE_NAME + '_test'
DB_SAVE_BATCH_SIZE = 3000
DB_READ_BATCH_SIZE = 1000

# test settings
TESTING = 'test' in sys.argv[0].split('/')[-1]

os.environ['CELERY_CONFIG_MODULE'] = 'bamboo.config.celeryconfig'

if TESTING:
    set_async(False)
    os.environ['CELERY_CONFIG_MODULE'] += '_test'

if len(sys.argv) > 1 and 'celeryconfig_test' in sys.argv[1]:
    DATABASE_NAME = TEST_DATABASE_NAME

RUN_PROFILER = False
