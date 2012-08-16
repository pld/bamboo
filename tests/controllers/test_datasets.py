import json
import os

import cherrypy

from controllers.datasets import Datasets
from controllers.calculations import Calculations
from lib.constants import CREATED_AT, ERROR, ID, MODE_INFO,\
    MODE_SUMMARY, MONGO_RESERVED_KEYS, MONGO_RESERVED_KEY_PREFIX,\
    SCHEMA, SUCCESS, SUMMARY, UPDATED_AT
from lib.decorators import requires_internet
from lib.io import create_dataset_from_url
from lib.utils import build_labels_to_slugs
from models.dataset import Dataset
from models.calculation import Calculation
from tests.test_base import TestBase
from tests.mock import MockUploadedFile


class TestDatasets(TestBase):

    # NOTE: NUM_COLS should be 15 (we are not currently returning _id column)
    NUM_COLS = 14
    NUM_ROWS = 19

    def setUp(self):
        TestBase.setUp(self)
        self._file_name = 'good_eats.csv'
        self._file_path = 'tests/fixtures/%s' % self._file_name
        self._file_uri = 'file://%s' % self._file_path
        self.url = 'http://formhub.org/mberg/forms/good_eats/data.csv'
        self._update_file_name = 'good_eats_update.json'
        self._update_file_path = 'tests/fixtures/%s' % self._update_file_name
        self.controller = Datasets()

    def _post_file(self):
        self.dataset_id = create_dataset_from_url(self._file_uri,
                                                  allow_local_file=True)[ID]

    def _post_calculations(self):
        # must call after _post_file
        controller = Calculations()
        formulae = [
            'amount + 1',
            'amount - 5',
        ]
        # NOTE: this may not work due to lack of synchronization
        for formula in formulae:
            controller.POST(self.dataset_id, formula, formula)

    def _test_summary_results(self, results):
        results = json.loads(results)
        self.assertTrue(isinstance(results, dict))
        return results

    def _test_summary_no_group(self, results):
        result_keys = results.keys()
        self.assertEqual(len(result_keys), self.NUM_COLS)
        columns = [col for col in
                   self.test_data[self._file_name].columns.tolist()
                   if not col in MONGO_RESERVED_KEYS]
        dataset = Dataset.find_one(self.dataset_id)
        labels_to_slugs = build_labels_to_slugs(dataset)
        for col in columns:
            slug = labels_to_slugs[col]
            self.assertTrue(slug in result_keys,
                            'col (slug): %s in: %s' % (slug, result_keys))
            self.assertTrue(SUMMARY in results[slug].keys())

    def _test_get_with_query_or_select(self, query='{}', select=None):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id, query=query,
                             select=select))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[3], dict))
        if select:
            self.assertEqual(sorted(results[0].keys()), ['rating'])
        if query != '{}':
            self.assertEqual(len(results), 11)

    def test_POST_dataset_id_update(self):
        self._post_file()
        self._post_calculations()
        num_rows = len(json.loads(self.controller.GET(self.dataset_id)))
        # mock the cherrypy server by setting the POST request body
        cherrypy.request.body = open(self._update_file_path, 'r')
        result = json.loads(self.controller.POST(dataset_id=self.dataset_id))
        num_rows_after_update = len(json.loads(
            self.controller.GET(self.dataset_id)))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)
        self.assertEqual(num_rows_after_update, num_rows + 1)
        results = json.loads(self.controller.GET(self.dataset_id))
        schema = json.loads(self.controller.GET(self.dataset_id,
                             mode=MODE_INFO))
        print schema[SCHEMA].keys()
        print results
        # TODO: deal with reserved keys
        temp_keys_to_ignore = [MONGO_RESERVED_KEY_PREFIX + key
            for key in MONGO_RESERVED_KEYS]
        for result in results:
            for column in schema[SCHEMA].keys():
                if column not in temp_keys_to_ignore:
                    self.assertTrue(column in result.keys(),
                        "column %s not in %s" % (column, result.keys()))
                    # TODO: check value somehow?

    def test_POST_file(self):
        _file = open(self._file_path, 'r')
        mock_uploaded_file = MockUploadedFile(_file)
        result = json.loads(self.controller.POST(csv_file=mock_uploaded_file))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)

    def test_POST_file_as_url_failure(self):
        result = json.loads(self.controller.POST(url=self._file_uri))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ERROR in result)

    @requires_internet
    def test_POST_url(self):
        result = json.loads(self.controller.POST(url=self.url))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)

    def test_POST_nonexistent_url(self):
        result = json.loads(self.controller.POST(url='http://noformhub.org/'))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ERROR in result)

    def test_POST_bad__url(self):
        result = json.loads(self.controller.POST(url='http://gooogle.com'))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ERROR in result)

    def test_GET(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        self.assertEqual(len(results), self.NUM_ROWS)

    def test_GET_schema(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=MODE_INFO))
        self.assertTrue(isinstance(results, dict))
        result_keys = results.keys()
        for key in [CREATED_AT, ID, SCHEMA, UPDATED_AT]:
            self.assertTrue(key in result_keys)

    def test_GET_bad_id(self):
        results = self.controller.GET('honey_badger')
        self.assertTrue(ERROR in results)

    def test_GET_unsupported_api_call(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id,
            'honey_badger'))
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
        results = self.controller.GET(self.dataset_id, mode=MODE_SUMMARY)
        results = self._test_summary_results(results)
        self._test_summary_no_group(results)

    def test_GET_summary_with_query(self):
        self._post_file()
        # (sic)
        results = self.controller.GET(self.dataset_id, mode=MODE_SUMMARY,
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
            json_results = self.controller.GET(self.dataset_id,
                                               mode=MODE_SUMMARY, group=group)
            results = self._test_summary_results(json_results)
            result_keys = results.keys()

            if len(column_values):
                self.assertTrue(group in result_keys, 'group: %s in: %s'
                                % (group, result_keys))
                self.assertEqual(column_values, results[group].keys())
                for column_value in column_values:
                    self._test_summary_no_group(results[group][column_value])
            else:
                self.assertFalse(group in results.keys())
                self.assertTrue(ERROR in results.keys())

    def test_GET_summary_with_group_and_query(self):
        self._post_file()
        results = self.controller.GET(self.dataset_id, mode=MODE_SUMMARY,
                                      group='rating',
                                      query='{"rating": "delectible"}')
        self._test_summary_results(results)

    def test_DELETE(self):
        self._post_file()
        result = json.loads(self.controller.DELETE(self.dataset_id))
        self.assertTrue(SUCCESS in result)
        self.assertEqual(result[SUCCESS], 'deleted dataset: %s' %
                         self.dataset_id)

    def test_DELETE_bad_id(self):
        for dataset_name in self.TEST_DATASETS:
            result = json.loads(self.controller.DELETE(
                                self.test_dataset_ids[dataset_name]))
            self.assertTrue(ERROR in result)
