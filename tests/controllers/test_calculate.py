import json

from controllers.calculate import Calculate
from controllers.datasets import Datasets
from tests.test_base import TestBase

class TestCalculate(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self._file = 'file://tests/fixtures/good_eats.csv'
        Datasets().POST(self._file)
        self.controller = Calculate()

    def test_GET(self):
        results = json.loads(self.controller.GET(self.digest))
        self.assertTrue(isinstance(results, dict))
        # TODO add more tests
