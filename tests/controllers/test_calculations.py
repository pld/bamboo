import json

from controllers.calculations import Calculations
from controllers.datasets import Datasets
from models.calculation import Calculation
from tests.test_base import TestBase

class TestCalculations(TestBase):

    def setUp(self):
        TestBase.setUp(self)

        self._file = 'file://tests/fixtures/good_eats.csv'
        result = json.loads(Datasets().POST(self._file))
        self.dataset_id = result['id']
        self.controller = Calculations()
        self.formula = 'x + y'
        self.name = 'test'

    def test_GET(self):
        self.controller.POST(self.dataset_id, self.formula, self.name)
        response = self.controller.GET(self.dataset_id)
        self.assertTrue(isinstance(json.loads(response), list))

    def test_POST(self):
        response = self.controller.POST(self.dataset_id, self.formula,
                self.name)
        self.assertTrue(isinstance(json.loads(response), dict))
