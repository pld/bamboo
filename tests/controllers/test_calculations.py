import json

from controllers.calculations import Calculations
from controllers.datasets import Datasets
from lib.constants import ALL
from tests.test_base import TestBase

class TestCalculations(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self._file = 'file://tests/fixtures/good_eats.csv'
        Datasets().POST(self._file)
        self.controller = Calculations()
        self.formula = 'x + y'
        self.name = 'test'

    def test_POST(self):
        self.controller.POST(self.dataset_id, self.formula, self.name)
        # TODO write me
