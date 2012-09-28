import json
import pickle

from controllers.calculations import Calculations
from controllers.datasets import Datasets
from models.dataset import Dataset
from lib.constants import ID, PARENT_DATASET_ID
from lib.utils import recognize_dates
from tests.controllers.test_abstract_datasets import TestAbstractDatasets


class TestDatasetsUpdate(TestAbstractDatasets):

    def setUp(self):
        """
        These tests use the following dataset configuration:

            d -> dataset
            m -> merged
            l -> linked

            d1   d2
             \  /  \
              m1    l1
                \  /
                 m2

        Dependencies flow from top to bottom.
        """
        TestAbstractDatasets.setUp(self)
        self.controller = Datasets()

        # create original datasets
        self._post_file()
        self.dataset1_id = self.dataset_id
        self._post_file()
        self.dataset2_id = self.dataset_id

        # create linked datasets
        self.calculations = Calculations()
        self.formula1 = 'sum(amount)'
        self.calculations.POST(self.dataset2_id, self.formula1, self.formula1)
        result = json.loads(
            self.controller.GET(self.dataset2_id, Datasets.MODE_RELATED))
        self.linked_dataset1_id = result['']

        # create merged datasets
        result = json.loads(self.controller.POST(
            merge=True,
            datasets=json.dumps([self.dataset1_id, self.dataset2_id])))
        self.merged_dataset1_id = result[ID]

        result = json.loads(self.controller.POST(
            merge=True,
            datasets=json.dumps(
                [self.merged_dataset1_id, self.linked_dataset1_id])))
        self.merged_dataset2_id = result[ID]

    def _verify_dataset(self, dataset_id, fixture_path):
        dframe = Dataset.find_one(dataset_id).observations(as_df=True)
        expected_dframe = recognize_dates(
            pickle.load(open(fixture_path, 'rb')))
        self._check_dframes_are_equal(dframe, expected_dframe)

    def test_setup_datasets(self):
        self._verify_dataset(self.dataset1_id,
                             'tests/fixtures/updates/originals/dataset1.p')
        self._verify_dataset(self.dataset2_id,
                             'tests/fixtures/updates/originals/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'tests/fixtures/updates/originals/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'tests/fixtures/updates/originals/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'tests/fixtures/updates/originals/merged_dataset2.p')

    def _test_update1(self):
        for dataset_id in [self.merged_dataset1_id, self.merged_dataset2_id]:
            merged_dataset = Dataset.find_one(dataset_id)
            merged_rows = merged_dataset.observations()
            for row in merged_rows:
                self.assertTrue(PARENT_DATASET_ID in row.keys())

        self._verify_dataset(self.dataset1_id,
                             'tests/fixtures/updates/update1/dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'tests/fixtures/updates/update1/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'tests/fixtures/updates/update1/merged_dataset2.p')

    def test_datasets_update1(self):
        self._post_row_updates(self.dataset1_id)
        self._test_update1()

    def test_datasets_update1_and_update2(self):
        self._post_row_updates(self.dataset1_id)
        self._test_update1()
        self._post_row_updates(self.dataset2_id)
        self._verify_dataset(
            self.merged_dataset1_id,
            'tests/fixtures/updates/update2/merged_dataset1.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'tests/fixtures/updates/update2/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'tests/fixtures/updates/update2/merged_dataset2.p')
