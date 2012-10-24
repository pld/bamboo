import json
import pickle

from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.core.frame import PARENT_DATASET_ID
from bamboo.models.dataset import Dataset
from bamboo.lib.datetools import recognize_dates
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasetsUpdateWithAggs(TestAbstractDatasets):

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

        # add calculations
        self.calculations = Calculations()
        self.calculations.create(
            self.dataset2_id, 'amount + gps_alt', 'amount plus gps_alt')

        # create linked datasets
        aggregations = {
            'max(amount)': 'max of amount',
            'mean(amount)': 'mean of amount',
            'median(amount)': 'median of amount',
            'min(amount)': 'min of amount',
            'ratio(amount, gps_latitude)': 'ratio of amount and gps_latitude',
            'sum(amount)': 'sum of amount',
        }

        for aggregation, name in aggregations.items():
            self.calculations.create(
                self.dataset2_id, aggregation, name)

        # and with group
        for aggregation, name in aggregations.items():
            self.calculations.create(
                self.dataset2_id, aggregation, name, group='food_type')

        result = json.loads(
            self.controller.related(self.dataset2_id))

        self.linked_dataset1_id = result['']

        # create merged datasets
        result = json.loads(self.controller.merge(
            datasets=json.dumps([self.dataset1_id, self.dataset2_id])))
        self.merged_dataset1_id = result[Dataset.ID]

        result = json.loads(self.controller.merge(
            datasets=json.dumps(
                [self.merged_dataset1_id, self.linked_dataset1_id])))
        self.merged_dataset2_id = result[Dataset.ID]

    def _verify_dataset(self, dataset_id, fixture_path):
        dframe = Dataset.find_one(dataset_id).dframe()
        expected_dframe = recognize_dates(
            pickle.load(open(fixture_path, 'rb')))
        self._check_dframes_are_equal(dframe, expected_dframe)

    def test_setup_datasets(self):
        self._verify_dataset(
            self.dataset1_id,
            'tests/fixtures/updates_with_aggs/originals/dataset1.p')
        self._verify_dataset(
            self.dataset2_id,
            'tests/fixtures/updates_with_aggs/originals/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'tests/fixtures/updates_with_aggs/originals/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'tests/fixtures/updates_with_aggs/originals/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'tests/fixtures/updates_with_aggs/originals/merged_dataset2.p')

    def test_datasets_update(self):
        self._put_row_updates(self.dataset2_id)
        self._verify_dataset(
            self.dataset2_id,
            'tests/fixtures/updates_with_aggs/update/dataset2.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'tests/fixtures/updates_with_aggs/update/merged_dataset1.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'tests/fixtures/updates_with_aggs/update/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'tests/fixtures/updates_with_aggs/update/merged_dataset2.p')
