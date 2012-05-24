import json

from controllers.datasets import Datasets
from lib.constants import ALL
from lib.decorators import requires_internet
from tests.test_base import TestBase

class TestDatasets(TestBase):

    NUM_COLS = 26
    ID_NOT_FOUND = 'id not found'

    def setUp(self):
        TestBase.setUp(self)
        self._file = 'file://tests/fixtures/good_eats.csv'
        self.url = 'http://formhub.org/mberg/forms/good_eats/data.csv'
        self.controller = Datasets()

    def _post_file(self):
        self.dataset_id = json.loads(self.controller.POST(self._file))['id']

    def _test_results(self, results):
        self.assertTrue(isinstance(results, dict))
        self.assertTrue(isinstance(results[ALL], list))
        self.assertEqual(len(results[ALL]), self.NUM_COLS)

    def test_POST_file(self):
        result = json.loads(self.controller.POST(self._file))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue('id' in result)

    @requires_internet
    def test_POST_url(self):
        result = json.loads(self.controller.POST(self.url))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue('id' in result)

    def test_POST_bad_url(self):
        result = json.loads(self.controller.POST('http://noformhub.org/'))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue('error' in result)

    def test_GET(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        self.assertEqual(len(results), 19)

    def test_GET_bad_id(self):
        results = self.controller.GET(self.dataset_id)
        self.assertEqual(results, self.ID_NOT_FOUND)

    def test_GET_with_query(self):
        self._post_file()
        # (sic)
        results = json.loads(self.controller.GET(self.dataset_id,
                    query='{"rating": "delectible"}'))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        self.assertEqual(len(results), 11)

    def test_GET_summary(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id, summary=True))
        self._test_results(results)

    def test_GET_summary_with_query(self):
        self._post_file()
        # (sic)
        results = json.loads(self.controller.GET(self.dataset_id, summary=True,
                    query='{"rating": "delectible"}'))
        self._test_results(results)

    def test_GET_summary_with_group(self):
        self._post_file()
        # (sic)
        results = json.loads(self.controller.GET(self.dataset_id, summary=True,
                    group='rating'))
        self._test_results(results)

    def test_GET_summary_with_group_and_query(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id, summary=True,
                    group='rating', query='{"rating": "delectible"}'))
        self._test_results(results)

    def test_DELETE(self):
        self._post_file()
        result = self.controller.DELETE(self.dataset_id)
        self.assertEqual(result, 'deleted dataset: %s' % self.dataset_id)

    def test_DELETE_bad_id(self):
        result = self.controller.DELETE(self.dataset_id)
        self.assertEqual(result, self.ID_NOT_FOUND)
