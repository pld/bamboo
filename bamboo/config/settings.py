import os
import sys

# database config
ASYNC_FLAG = 'BAMBOO_ASYNC_OFF'
DATABASE_NAME = 'bamboo_dev'
TEST_DATABASE_NAME = DATABASE_NAME + '_test'
DB_SAVE_BATCH_SIZE = 3000
DB_READ_BATCH_SIZE = 1000

# test settings
TESTING = 'test' in sys.argv[0].split('/')[-1]


def is_async():
    return not os.getenv(ASYNC_FLAG)


def set_async(on):
    if on:
        if not is_async():
            del os.environ[ASYNC_FLAG]
    else:
        os.environ[ASYNC_FLAG] = 'True'


os.environ['CELERY_CONFIG_MODULE'] = 'bamboo.config.celeryconfig'

if TESTING:
    set_async(False)
    os.environ['CELERY_CONFIG_MODULE'] += '_test'

if len(sys.argv) > 1 and 'celeryconfig_test' in sys.argv[1]:
    DATABASE_NAME = TEST_DATABASE_NAME

RUN_PROFILER = False
