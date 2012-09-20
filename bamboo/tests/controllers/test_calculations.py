import json

from controllers.abstract_controller import AbstractController
from controllers.calculations import Calculations
from controllers.datasets import Datasets
from lib.constants import ALL, DATASET_ID, ERROR, ID
from lib.io import create_dataset_from_url
from models.calculation import Calculation
from models.dataset import Dataset
from tests.test_base import TestBase


class TestCalculations(TestBase):

    def setUp(self):
        TestBase.setUp(self)

        self._file = 'file://tests/fixtures/good_eats.csv'
        self.dataset_id = create_dataset_from_url(self._file,
                                                  allow_local_file=True)[ID]
        self.controller = Calculations()
        self.formula = 'amount + gps_alt'
        self.name = 'test'

    def _post_formula(self):
        return self.controller.POST(self.dataset_id, self.formula, self.name)

    def test_GET(self):
        self._post_formula()
        response = self.controller.GET(self.dataset_id)
        self.assertTrue(isinstance(json.loads(response), list))

    def test_POST(self):
        response = json.loads(self._post_formula())
        self.assertTrue(isinstance(response, dict))
        self.assertFalse(DATASET_ID in response)

    def test_POST_remove_summary(self):
        Datasets().GET(
            self.dataset_id,
            mode=Datasets.MODE_SUMMARY,
            select=Datasets.SELECT_ALL_FOR_SUMMARY)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(isinstance(dataset.stats, dict))
        self.assertTrue(isinstance(dataset.stats[ALL], dict))
        self._post_formula()
        # stats should have new column for calculation
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(self.name in dataset.stats.get(ALL).keys())

    def test_DELETE_nonexistent_calculation(self):
        result = json.loads(self.controller.DELETE(self.dataset_id, self.name))
        self.assertTrue(ERROR in result)

    def test_DELETE(self):
        self._post_formula()
        result = json.loads(self.controller.DELETE(self.dataset_id, self.name))
        self.assertTrue(AbstractController.SUCCESS in result)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(self.name not in dataset.build_labels_to_slugs())
