import pickle

from bamboo.controllers.calculations import Calculations
from bamboo.lib.datetools import recognize_dates
from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestAbstractDatasetsUpdate(TestAbstractDatasets):

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

    def _create_original_datasets(self):
        self.dataset1_id = self._post_file()
        self.dataset2_id = self._post_file()

    def _add_common_calculations(self):
        self.calculations = Calculations()
        self.calculations.create(
            self.dataset2_id, 'amount + gps_alt', 'amount plus gps_alt')

    def _verify_dataset(self, dataset_id, fixture_path):
        dframe = Dataset.find_one(dataset_id).dframe()
        expected_dframe = recognize_dates(
            pickle.load(open('%s%s' % (
                self.FIXTURE_PATH, fixture_path), 'rb')))
        self._check_dframes_are_equal(dframe, expected_dframe)
