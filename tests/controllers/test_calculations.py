import json

from controllers.calculations import Calculations
from controllers.datasets import Datasets
from lib.constants import ALL, ID, STATS
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
        response = self._post_formula()
        self.assertTrue(isinstance(json.loads(response), dict))

    def test_POST_remove_summary(self):
        Datasets().GET(self.dataset_id, summary=True)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(isinstance(dataset[STATS], dict))
        self.assertTrue(isinstance(dataset[STATS][ALL], dict))
        self._post_formula()
        # [STATS][ALL] should be removed
        dataset = Dataset.find_one(self.dataset_id)
        self.assertEqual(dataset[STATS].get(ALL), None)
