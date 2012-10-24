import json
import pickle

from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.core.frame import PARENT_DATASET_ID
from bamboo.models.dataset import Dataset
from bamboo.lib.datetools import recognize_dates
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasetsUpdateWithCalcs(TestAbstractDatasets):

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
        self.calculations.create(
            self.dataset2_id, 'sum(amount)', 'sum of amount')
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
            'tests/fixtures/updates_with_calcs/originals/dataset1.p')
        self._verify_dataset(
            self.dataset2_id,
            'tests/fixtures/updates_with_calcs/originals/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'tests/fixtures/updates_with_calcs/originals/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'tests/fixtures/updates_with_calcs/originals/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'tests/fixtures/updates_with_calcs/originals/merged_dataset2.p')

    def _add_calculations(self):
        self.calculations.create(self.dataset2_id,
                                 'amount_plus_gps_alt > gps_precision',
                                 'amount plus gps_alt > gps_precision')
        self.calculations.create(self.linked_dataset1_id,
                                 'sum_of_amount * 2',
                                 'amount')
        self.calculations.create(self.merged_dataset1_id,
                                 'gps_alt * 2',
                                 'double gps_alt')
        self.calculations.create(self.merged_dataset2_id,
                                 'amount * 2',
                                 'double amount')
        self._verify_dataset(
            self.dataset2_id,
            'tests/fixtures/updates_with_calcs/calcs/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'tests/fixtures/updates_with_calcs/calcs/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'tests/fixtures/updates_with_calcs/calcs/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'tests/fixtures/updates_with_calcs/calcs/merged_dataset2.p')

    def test_datasets_add_calculations(self):
        self._add_calculations()
