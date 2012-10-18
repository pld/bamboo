import os
import sys

# database config
DATABASE_NAME = 'bamboo_dev'
TEST_DATABASE_NAME = DATABASE_NAME + '_test'
DB_BATCH_SIZE = 1000

# test settings
TESTING = 'test' in sys.argv[0].split('/')[-1]

os.environ['CELERY_CONFIG_MODULE'] = 'bamboo.config.celeryconfig'

if TESTING:
    os.environ['BAMBOO_ASYNC_OFF'] = 'True'
    os.environ['CELERY_CONFIG_MODULE'] += '_test'

if len(sys.argv) > 1 and 'celeryconfig_test' in sys.argv[1]:
    DATABASE_NAME = TEST_DATABASE_NAME

RUN_PROFILER = False

# run async?
ASYNCHRONOUS_TASKS = True
