import os
import unittest
import uuid

from pandas import read_csv

from bamboo.config.db import Database
from bamboo.config.settings import TEST_DATABASE_NAME


class TestBase(unittest.TestCase):

    FIXTURE_PATH = 'tests/fixtures/'
    SLEEP_DELAY = 0.2
    TEST_DATASETS = [
        'good_eats.csv',
        'good_eats_large.csv',
        'good_eats_with_calculations.csv',
        'kenya_secondary_schools_2007.csv',
        'soil_samples.csv',
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
        Database.db(TEST_DATABASE_NAME)

    def _drop_database(self):
        Database.connection().drop_database(TEST_DATABASE_NAME)

    def _local_fixture_prefix(self):
        return 'file://localhost%s/tests/fixtures/' % os.getcwd()

    def _load_test_data(self):
        for dataset_name in self.TEST_DATASETS:
            self.test_data[dataset_name] = read_csv(
                '%s%s' % (self._local_fixture_prefix(), dataset_name))
            self.test_dataset_ids[dataset_name] = uuid.uuid4().hex
