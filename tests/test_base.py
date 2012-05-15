import unittest
import uuid

from pandas import read_csv

from config.db import Database
from lib.io import open_data_file
from tests.celeryd import CeleryDaemon


class TestBase(unittest.TestCase):

    TEST_DATABASE_NAME = 'bamboo_test'
    CELERYD_PID_FILE = '/tmp/celerydd.pid'

    def setUp(self):
        self._drop_database()
        self._create_database()
        self._start_celery()
        self._load_test_data()

    def tearDown(self):
        self._drop_database()
        self._stop_celery()

    def _create_database(self):
        Database.db(self.TEST_DATABASE_NAME)

    def _drop_database(self):
        Database.connection().drop_database(self.TEST_DATABASE_NAME)

    def _start_celery(self):
        self.celeryd = CeleryDaemon('config.celeryconfig_test', self.CELERYD_PID_FILE)
        self.celeryd.start()

    def _stop_celery(self):
        self.celeryd.stop()

    def _load_test_data(self):
        f = open_data_file('file://tests/fixtures/good_eats.csv')
        self.data = read_csv(f, na_values=['n/a'])
        self.dataset_id = uuid.uuid4().hex

    def _load_calculation(self):
        self.formula = 'x + y'
        self.name = 'test'
