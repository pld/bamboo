import os
import sys

# database config
DB_BATCH_SIZE = 1000
DATABASE_NAME = 'bamboo_dev'
TEST_DATABASE_NAME = DATABASE_NAME + '_test'

# check if we are in testing mode
TESTING = False
if 'test' in sys.argv[0].split('/')[-1] or os.environ.get('BAMBOO_TESTING') == 'True':
    TESTING = True

# test settings
if TESTING:
    os.environ['CELERY_CONFIG_MODULE'] = 'bamboo.config.celeryconfig_test'
    os.environ['BAMBOO_ASYNC_OFF'] = 'True'
    os.environ['BAMBOO_TESTING'] = 'True'
    DATABASE_NAME = TEST_DATABASE_NAME

RUN_PROFILER = False

# run async?
ASYNCHRONOUS_TASKS = True
