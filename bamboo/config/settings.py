import os
import sys

# database config
DATABASE_NAME = 'bamboo_dev'
TEST_DATABASE_NAME = DATABASE_NAME + '_test'
DB_SAVE_BATCH_SIZE = 3000
DB_READ_BATCH_SIZE = 1000

# test settings
TESTING = 'test' in sys.argv[0].split('/')[-1]


def set_async(on):
    async_flag = 'BAMBOO_ASYNC_OFF'

    if on:
        if os.environ.get(async_flag):
            del os.environ[async_flag]
    else:
        os.environ[async_flag] = '1'


def is_async():
    return not os.getenv('BAMBOO_ASYNC_OFF')


os.environ['CELERY_CONFIG_MODULE'] = 'bamboo.config.celeryconfig'

if TESTING:
    set_async(False)
    os.environ['CELERY_CONFIG_MODULE'] += '_test'

if len(sys.argv) > 1 and 'celeryconfig_test' in sys.argv[1]:
    DATABASE_NAME = TEST_DATABASE_NAME

RUN_PROFILER = False
