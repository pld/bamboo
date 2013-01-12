import os
from time import sleep
import unittest
import uuid

from pandas import read_csv

from bamboo.config.db import Database
from bamboo.config.settings import TEST_DATABASE_NAME
from bamboo.lib.io import create_dataset_from_csv
from bamboo.models.dataset import Dataset
from bamboo.tests.mock import MockUploadedFile


class TestBase(unittest.TestCase):

    FIXTURE_PATH = 'tests/fixtures/'
    SLEEP_DELAY = 0.2
    TEST_DATASETS = [
        'good_eats.csv',
        'good_eats_large.csv',
        'good_eats_with_calculations.csv',
        'kenya_secondary_schools_2007.csv',
        'soil_samples.csv',
        'water_points.csv',
    ]

    test_data = {}
    test_dataset_ids = {}

    def setUp(self):
        self._drop_database()
        self._create_database()
        self._load_test_data()

    def tearDown(self):
        self._drop_database()

    def get_data(self, dataset_name):
        data = self.test_data.get(dataset_name)
        if data is None:
            data = self.test_data[dataset_name] = read_csv(
                '%s%s' % (self._local_fixture_prefix(), dataset_name))
        return data

    def _create_database(self):
        Database.db(TEST_DATABASE_NAME)

    def _drop_database(self):
        Database.connection().drop_database(TEST_DATABASE_NAME)

    def _local_fixture_prefix(self, filename=''):
        return 'file://localhost%s/tests/fixtures/%s' % (os.getcwd(), filename)

    def _fixture_path_prefix(self, filename=''):
        return '/%s/tests/fixtures/%s' % (os.getcwd(), filename)

    def _load_test_data(self):
        for dataset_name in self.TEST_DATASETS:
            self.test_dataset_ids[dataset_name] = uuid.uuid4().hex

    def _file_mock(self, file_path):
        _file = open(file_path, 'r')
        return MockUploadedFile(_file)

    def _post_file(self, file_name='good_eats.csv'):
        return create_dataset_from_csv(
            self._file_mock(self._fixture_path_prefix(file_name))).dataset_id

    def _wait_for_dataset_state(self, dataset_id):
        while True:
            dataset = Dataset.find_one(dataset_id)

            if dataset.state != Dataset.STATE_PENDING:
                break

            sleep(self.SLEEP_DELAY)

        return dataset
