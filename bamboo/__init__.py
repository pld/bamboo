import os
import sys

from bamboo.core.frame import *  # nopep8
from bamboo.lib.async import *  # nopep8


# test settings
TESTING = 'test' in sys.argv[0].split('/')[-1]

os.environ['CELERY_CONFIG_MODULE'] = 'bamboo.config.celeryconfig'

if TESTING:
    set_async(False)
    os.environ['CELERY_CONFIG_MODULE'] += '_test'

# perform import after env variables are set
from bamboo.models.dataset import Dataset  # nopep8
