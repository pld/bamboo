import json

from controllers.calculate import Calculate
from controllers.datasets import Datasets
from lib.constants import ALL
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
        self.assertTrue(isinstance(results[ALL], list))

    def test_GET_with_group(self):
        results = json.loads(self.controller.GET(self.digest, u'food_type'))
        self.assertTrue(isinstance(results, dict))
        self.assertTrue(isinstance(results['caffeination'], list))

    def test_GET_with_query(self):
        # TODO write me
        pass
