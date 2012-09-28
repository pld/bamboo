from config import settings

BROKER_URL = 'mongodb://localhost:27017/%s' % settings.DATABASE_NAME
cELERY_RESULT_BACKEND = 'mongodb'
CELERY_MONGODB_BACKEND_SETTINGS = {
    'host': 'localhost',
    'port': 27017,
    'database': settings.DATABASE_NAME,
    'taskmeta_collection': 'celery_tasks',
}
CELERY_IMPORTS = ('lib.calculator', )
CELERY_ALWAYS_EAGER = True

# poor man's synchonization
# TODO: proper synchronization!
CELERYD_CONCURRENCY = 1
