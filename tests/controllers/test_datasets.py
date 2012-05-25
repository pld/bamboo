import json

from controllers.datasets import Datasets
from lib.constants import ALL
from lib.decorators import requires_internet
from tests.test_base import TestBase

class TestDatasets(TestBase):

    NUM_COLS = 26

    def setUp(self):
        TestBase.setUp(self)
        self._file = 'file://tests/fixtures/good_eats.csv'
        self.url = 'http://formhub.org/mberg/forms/good_eats/data.csv'
        self.controller = Datasets()

    def _post_file(self):
        self.dataset_id = json.loads(self.controller.POST(self._file))['id']

    def _test_results(self, results):
        results = json.loads(results)
        self.assertTrue(isinstance(results, dict))
        self.assertTrue(isinstance(results[ALL], list))
        self.assertEqual(len(results[ALL]), self.NUM_COLS)

    def _test_get_with_query_or_select(self, query='{}', select=None):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id, query=query,
                    select=select))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        if select:
            self.assertEqual(sorted(results[0].keys()), ['_id', 'rating'])
        if query != '{}':
            self.assertEqual(len(results), 11)

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
        self.assertTrue('error' in results)

    def test_GET_with_query(self):
        # (sic)
        self._test_get_with_query_or_select('{"rating": "delectible"}')

    def test_GET_with_bad_query(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id,
                    query='bad json'))
        self.assertTrue('JSON' in results['error'])

    def test_GET_with_select(self):
        self._test_get_with_query_or_select(select='{"rating": 1}')

    def test_GET_with_select_and_query(self):
        self._test_get_with_query_or_select('{"rating": "delectible"}',
                '{"rating": 1}')

    def test_GET_summary(self):
        self._post_file()
        results = self.controller.GET(self.dataset_id, summary=True)
        self._test_results(results)

    def test_GET_summary_with_query(self):
        self._post_file()
        # (sic)
        results = self.controller.GET(self.dataset_id, summary=True,
                    query='{"rating": "delectible"}')
        self._test_results(results)

    def test_GET_summary_with_group(self):
        self._post_file()
        # (sic)
        results = self.controller.GET(self.dataset_id, summary=True,
                    group='rating')
        self._test_results(results)

    def test_GET_summary_with_group_and_query(self):
        self._post_file()
        results = self.controller.GET(self.dataset_id, summary=True,
                    group='rating', query='{"rating": "delectible"}')
        self._test_results(results)

    def test_DELETE(self):
        self._post_file()
        result = json.loads(self.controller.DELETE(self.dataset_id))
        self.assertTrue('success' in result)
        self.assertEqual(result['success'], 'deleted dataset: %s' % self.dataset_id)

    def test_DELETE_bad_id(self):
        result = json.loads(self.controller.DELETE(self.dataset_id))
        self.assertTrue('error' in result)
