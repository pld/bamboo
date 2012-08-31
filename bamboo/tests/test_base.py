import unittest
import uuid

from pandas import read_csv

from config.db import Database
from lib.io import open_data_file


class TestBase(unittest.TestCase):

    TEST_DATABASE_NAME = 'bamboo_test'
    TEST_DATASETS = [
        'good_eats.csv',
        'good_eats_large.csv',
        'good_eats_with_calculations.csv',
        'kenya_secondary_schools_2007.csv',
    ]

    test_data = {}
    test_dataset_ids = {}

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
        for dataset_name in self.TEST_DATASETS:
            f = open_data_file('file://tests/fixtures/%s' % dataset_name,
                               allow_local_file=True)
            self.test_data[dataset_name] = read_csv(f)
            self.test_dataset_ids[dataset_name] = uuid.uuid4().hex
