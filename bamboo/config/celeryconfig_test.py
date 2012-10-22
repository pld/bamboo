from bamboo.config import settings

BROKER_BACKEND = 'mongodb'
BROKER_URL = 'mongodb://localhost:27017/%s' % settings.TEST_DATABASE_NAME
CELERY_RESULT_BACKEND = 'mongodb'
CELERY_MONGODB_BACKEND_SETTINGS = {
    'host': 'localhost',
    'port': 27017,
    'database': settings.TEST_DATABASE_NAME,
    'taskmeta_collection': 'celery_tasks',
}
CELERY_IMPORTS = ('bamboo.lib.io', 'bamboo.core.merge')
CELERYD_CONCURRENCY = 1
