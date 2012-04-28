import json

from controllers.datasets import Datasets
from tests.decorators import requires_internet
from tests.test_base import TestBase

class TestDatasets(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self._file = 'file://tests/fixtures/good_eats.csv'
        self.url = 'http://formhub.org/mberg/forms/good_eats/data.csv'
        self.controller = Datasets()

    def test_POST_file(self):
        result = json.loads(self.controller.POST(self._file))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue('id' in result)

    @requires_internet
    def test_POST_url(self):
        result = json.loads(self.controller.POST(self.url))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue('id' in result)

    def test_GET(self):
        _id = json.loads(self.controller.POST(self._file))['id']
        results = json.loads(self.controller.GET(_id))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        self.assertEqual(len(results), 19)

    def test_GET_with_query(self):
        # TODO write me
        pass

    def test_DELETE(self):
        result = json.loads(self.controller.POST(self.url))
        self.controller.DELETE(result['id'])
