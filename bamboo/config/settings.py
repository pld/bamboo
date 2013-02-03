import sys


# database config
ASYNC_FLAG = 'BAMBOO_ASYNC_OFF'
DATABASE_NAME = 'bamboo_dev'
TEST_DATABASE_NAME = DATABASE_NAME + '_test'


if len(sys.argv) > 1 and 'celeryconfig_test' in sys.argv[1]:
    DATABASE_NAME = TEST_DATABASE_NAME

RUN_PROFILER = False
