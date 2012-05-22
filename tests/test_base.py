import unittest
import uuid

from pandas import read_csv

from config.db import Database
from lib.io import open_data_file


class TestBase(unittest.TestCase):

    TEST_DATABASE_NAME = 'bamboo_test'

    def setUp(self):
        self._drop_database()
        self._create_database()
        self._load_test_data()

    def tearDown(self):
        self._drop_database()

    def _create_database(self):
        Database.db(self.TEST_DATABASE_NAME)

    def _drop_database(self):
        Database.connection().drop_database(self.TEST_DATABASE_NAME)

    def _load_test_data(self):
        f = open_data_file('file://tests/fixtures/good_eats.csv')
        self.data = read_csv(f)
        self.dataset_id = uuid.uuid4().hex
