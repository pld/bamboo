import unittest
import uuid

from pandas import read_csv

from config.db import Database
from lib.utils import open_data_file


class TestBase(unittest.TestCase):

    def setUp(self):
        self._drop_database()
        self._create_database()
        self._load_test_data()

    def tearDown(self):
        self._drop_database()

    def _create_database(self):
        Database.db('bamboo_test')

    def _drop_database(self):
        Database.connection().drop_database('bamboo_test')

    def _load_test_data(self):
        f = open_data_file('file://tests/fixtures/good_eats.csv')
        self.data = read_csv(f)#, na_values=['n/a'])
        self.dataset_id = uuid.uuid4().hex
