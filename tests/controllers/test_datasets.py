import json

from controllers.datasets import Datasets
from lib.constants import ERROR, ID, MONGO_RESERVED_KEYS, SUCCESS, SUMMARY
from lib.decorators import requires_internet
from lib.io import create_dataset_from_url
from tests.test_base import TestBase
from tests.mock import MockUploadedFile

class TestDatasets(TestBase):

    NUM_COLS = 43

    def setUp(self):
        TestBase.setUp(self)
        self._file_name = 'good_eats.csv'
        self._file_path = 'tests/fixtures/%s' % self._file_name
        self._file_uri = 'file://%s' % self._file_path
        self.url = 'http://formhub.org/mberg/forms/good_eats/data.csv'
        self.controller = Datasets()

    def _post_file(self):
        self.dataset_id = create_dataset_from_url(self._file_uri,
                allow_local_file=True)[ID]

    def _test_summary_results(self, results):
        results = json.loads(results)
        self.assertTrue(isinstance(results, dict))
        return results

    def _test_summary_no_group(self, results):
        result_keys = results.keys()
        print result_keys
        print self.test_data[self._file_name].columns.tolist()
        self.assertEqual(len(result_keys), self.NUM_COLS)
        columns = [col for col in
                self.test_data[self._file_name].columns.tolist()
                if not col in MONGO_RESERVED_KEYS]
        for col in columns:
            self.assertTrue(col in result_keys, 'col: %s in: %s' % (col,
                        result_keys))
            self.assertTrue(SUMMARY in results[col].keys())

    def _test_get_with_query_or_select(self, query='{}', select=None):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id, query=query,
                    select=select))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        if select:
            self.assertEqual(sorted(results[0].keys()), ['rating'])
        if query != '{}':
            self.assertEqual(len(results), 11)

    def test_POST_file(self):
        _file = open(self._file_path, 'r')
        mock_uploaded_file = MockUploadedFile(_file)
        result = json.loads(self.controller.POST(csv_file=mock_uploaded_file))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)

    def test_POST_file_as_url_failure(self):
        result = json.loads(self.controller.POST(self._file_uri))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ERROR in result)

    @requires_internet
    def test_POST_url(self):
        result = json.loads(self.controller.POST(self.url))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)

    def test_POST_bad_url(self):
        result = json.loads(self.controller.POST('http://noformhub.org/'))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ERROR in result)

    def test_GET(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        self.assertEqual(len(results), 19)

    def test_GET_bad_id(self):
        for dataset_name in self.TEST_DATASETS:
            results = self.controller.GET(self.test_dataset_ids[dataset_name])
            self.assertTrue(ERROR in results)

    def test_GET_with_query(self):
        # (sic)
        self._test_get_with_query_or_select('{"rating": "delectible"}')

    def test_GET_with_bad_query(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id,
                    query='bad json'))
        self.assertTrue('JSON' in results[ERROR])

    def test_GET_with_select(self):
        self._test_get_with_query_or_select(select='{"rating": 1}')

    def test_GET_with_select_and_query(self):
        self._test_get_with_query_or_select('{"rating": "delectible"}',
                '{"rating": 1}')

    def test_GET_summary(self):
        self._post_file()
        results = self.controller.GET(self.dataset_id, summary=True)
        results = self._test_summary_results(results)
        self._test_summary_no_group(results)

    def test_GET_summary_with_query(self):
        self._post_file()
        # (sic)
        results = self.controller.GET(self.dataset_id, summary=True,
                    query='{"rating": "delectible"}')
        results = self._test_summary_results(results)
        self._test_summary_no_group(results)

    def test_GET_summary_with_group(self):
        self._post_file()
        groups = [
            ('rating', ['delectible', 'epic_eat']),
            ('amount', []),
        ]

        for group, column_values in groups:
            json_results = self.controller.GET(self.dataset_id, summary=True,
                        group=group)
            results = self._test_summary_results(json_results)
            result_keys = results.keys()

            if len(column_values):
                self.assertTrue(group in result_keys, 'group: %s in: %s'
                        % (group, result_keys))
                self.assertEqual(column_values, results[group].keys())
                for column_value in column_values:
                    self._test_summary_no_group(results[group][column_value])
            else:
                print results.keys()
                self.assertFalse(group in results.keys())
                self.assertTrue(ERROR in results.keys())

    def test_GET_summary_with_group_and_query(self):
        self._post_file()
        results = self.controller.GET(self.dataset_id, summary=True,
                    group='rating', query='{"rating": "delectible"}')
        self._test_summary_results(results)

    def test_DELETE(self):
        self._post_file()
        result = json.loads(self.controller.DELETE(self.dataset_id))
        self.assertTrue(SUCCESS in result)
        self.assertEqual(result[SUCCESS], 'deleted dataset: %s' % \
                self.dataset_id)

    def test_DELETE_bad_id(self):
        for dataset_name in self.TEST_DATASETS:
            result = json.loads(self.controller.DELETE(
                        self.test_dataset_ids[dataset_name]))
            self.assertTrue(ERROR in result)
