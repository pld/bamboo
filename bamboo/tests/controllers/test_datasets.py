from datetime import datetime
import os
import pickle
import simplejson as json
from time import mktime

import numpy as np
from pandas import concat

from controllers.abstract_controller import AbstractController
from controllers.datasets import Datasets
from controllers.calculations import Calculations
from lib.constants import DATETIME, DIMENSION, ERROR,\
    ID, MONGO_RESERVED_KEYS, MONGO_RESERVED_KEY_PREFIX,\
    MONGO_RESERVED_KEY_STRS, NUM_COLUMNS, NUM_ROWS,\
    PARENT_DATASET_ID, SCHEMA, SIMPLETYPE, SUMMARY
from lib.decorators import requires_internet
from lib.utils import GROUP_DELIMITER
from models.dataset import Dataset
from models.calculation import Calculation
from tests.controllers.test_abstract_datasets import TestAbstractDatasets
from tests.mock import MockUploadedFile


class TestDatasets(TestAbstractDatasets):

    NUM_COLS = 15
    NUM_ROWS = 19

    def setUp(self):
        TestAbstractDatasets.setUp(self)
        self._file_path = 'tests/fixtures/%s' % self._file_name
        self._file_uri = 'file://%s' % self._file_path
        self.url = 'http://formhub.org/mberg/forms/good_eats/data.csv'
        self._file_name_with_slashes = 'good_eats_with_slashes.csv'
        self.default_formulae = [
            'amount',
            'amount + 1',
            'amount - 5',
        ]
        self.cardinalities = pickle.load(
            open('tests/fixtures/good_eats_cardinalities.p', 'rb'))
        self.simpletypes = pickle.load(
            open('tests/fixtures/good_eats_simpletypes.p', 'rb'))

    def _post_calculations(self, formulae=[], group=None):
        # must call after _post_file
        controller = Calculations()
        for idx, formula in enumerate(formulae):
            name = 'calc_%d' % idx if formula in self.schema.keys()\
                else formula
            controller.POST(self.dataset_id, formula, name, group)

    def _test_summary_results(self, results):
        results = json.loads(results)
        self.assertTrue(isinstance(results, dict))
        return results

    def _test_summary_no_group(self, results, group=None):
        group = [group] if group else []
        result_keys = results.keys()
        # minus the column that we are grouping on
        self.assertEqual(len(result_keys), self.NUM_COLS - len(group))
        columns = [col for col in
                   self.test_data[self._file_name].columns.tolist()
                   if not col in MONGO_RESERVED_KEYS + group]
        dataset = Dataset.find_one(self.dataset_id)
        labels_to_slugs = dataset.build_labels_to_slugs()
        for col in columns:
            slug = labels_to_slugs[col]
            self.assertTrue(slug in result_keys,
                            'col (slug): %s in: %s' % (slug, result_keys))
            self.assertTrue(SUMMARY in results[slug].keys())

    def _test_summary_built(self, result):
        # check that summary is created
        self.dataset_id = result[ID]
        results = self.controller.GET(
            self.dataset_id,
            mode=Datasets.MODE_SUMMARY,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)
        return self._test_summary_results(results)

    def _test_get_with_query_or_select(self, query='{}', select=None,
                                       num_results=None, result_keys=None):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id, query=query,
                             select=select))
        self.assertTrue(isinstance(results, list))
        if num_results > 3:
            self.assertTrue(isinstance(results[3], dict))
        if select:
            self.assertEqual(sorted(results[0].keys()), result_keys)
        if query != '{}':
            self.assertEqual(len(results), num_results)

    def _test_mode_related(self, groups=['']):
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_RELATED))
        self.assertTrue(isinstance(results, dict))
        self.assertEqual(len(results.keys()), len(groups))
        self.assertEqual(results.keys(), groups)
        linked_dataset_id = results[groups[0]]
        self.assertTrue(isinstance(linked_dataset_id, basestring))

        # inspect linked dataset
        return json.loads(self.controller.GET(linked_dataset_id))

    def test_POST_dataset_id_update_bad_dataset_id(self):
        result = json.loads(self.controller.PUT(dataset_id=111))
        assert(ERROR in result)

    def test_POST_dataset_id_update(self):
        self._post_file(self._file_name_with_slashes)
        self._post_calculations(self.default_formulae)
        num_rows = len(json.loads(self.controller.GET(self.dataset_id)))
        self._post_row_updates()
        results = json.loads(self.controller.GET(self.dataset_id))
        num_rows_after_update = len(results)
        self.assertEqual(num_rows_after_update, num_rows + 1)
        for result in results:
            for column in self.schema.keys():
                self.assertTrue(
                    column in result.keys(),
                    "column %s not in %s" % (column, result.keys()))
        # compare the last column values with what we expect
        for key, value in results[-1].items():
            self.assertEqual(value, self._update_values[key],
                             'error in column: %s' % key)

    def test_POST_dataset_id_update_with_aggregation(self):
        self._post_file()
        self._post_calculations(
            formulae=self.default_formulae + ['sum(amount)'])
        num_rows = len(json.loads(self.controller.GET(self.dataset_id)))
        self._post_row_updates()
        results = json.loads(self.controller.GET(self.dataset_id))
        num_rows_after_update = len(results)
        self.assertEqual(num_rows_after_update, num_rows + 1)
        for result in results:
            for column in self.schema.keys():
                self.assertTrue(
                    column in result.keys(),
                    "column %s not in %s" % (column, result.keys()))
        self._test_mode_related()

    def test_POST_file(self):
        _file = open(self._file_path, 'r')
        mock_uploaded_file = MockUploadedFile(_file)
        result = json.loads(self.controller.POST(csv_file=mock_uploaded_file))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)

        results = self._test_summary_built(result)
        self._test_summary_no_group(results)

    def test_POST_file_for_nan_float_cell(self):
        """First data row has one cell blank, which is usually interpreted
        as nan, a float value."""
        _file_name = 'good_eats_nan_float.csv'
        _file_path = self._file_path.replace(self._file_name, _file_name)
        _file = open(_file_path, 'r')
        mock_uploaded_file = MockUploadedFile(_file)
        result = json.loads(self.controller.POST(csv_file=mock_uploaded_file))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)

        results = self._test_summary_built(result)
        self._test_summary_no_group(results)
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_INFO))

        for column_name, column_schema in results[SCHEMA].items():
            self.assertEqual(
                column_schema[SIMPLETYPE], self.simpletypes[column_name])

    def test_POST_file_as_url_failure(self):
        result = json.loads(self.controller.POST(url=self._file_uri))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ERROR in result)

    @requires_internet
    def test_POST_url(self):
        result = json.loads(self.controller.POST(url=self.url))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)

        self._test_summary_built(result)

    @requires_internet
    def test_POST_not_csv_url(self):
        result = json.loads(self.controller.POST(url='http://74.125.228.110/'))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ERROR in result)

    @requires_internet
    def test_POST_bad_url(self):
        result = json.loads(self.controller.POST(url='http://dsfskfjdks.com'))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ERROR in result)

    def test_POST_merge_datasets(self):
        self._post_file()
        dataset_id1 = self.dataset_id
        self._post_file()
        dataset_id2 = self.dataset_id
        result = json.loads(self.controller.POST(
            merge=True,
            datasets=json.dumps([dataset_id1, dataset_id2])))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)

        datasets = [Dataset.find_one(dataset_id)
                    for dataset_id in [dataset_id1, dataset_id2]]

        for dataset in datasets:
            self.assertTrue(result[ID] in dataset.merged_dataset_ids)

        dframe1 = datasets[0].observations(as_df=True)
        merged_dataset = Dataset.find_one(result[ID])
        merged_rows = merged_dataset.observations()
        for row in merged_rows:
            self.assertTrue(PARENT_DATASET_ID in row.keys())
        merged_dframe = merged_dataset.observations(as_df=True)

        self.assertEqual(len(merged_dframe), 2 * len(dframe1))
        expected_dframe = concat([dframe1, dframe1],
                                 ignore_index=True)
        self.assertEqual(list(merged_dframe.columns),
                         list(expected_dframe.columns))

        self._check_dframes_are_equal(merged_dframe, expected_dframe)

    def test_GET(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        self.assertEqual(len(results), self.NUM_ROWS)

    def test_GET_after_calculation(self):
        self._post_file()
        self._post_calculations(['amount < 4'])
        results = json.loads(self.controller.GET(self.dataset_id,
                             select='{"amount___4": 1}'))
        self.assertTrue(isinstance(results, list))
        self.assertTrue(isinstance(results[0], dict))
        self.assertEqual(len(results), self.NUM_ROWS)

    def test_GET_info(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_INFO))
        self.assertTrue(isinstance(results, dict))
        self.assertTrue(SCHEMA in results.keys())
        self.assertTrue(NUM_ROWS in results.keys())
        self.assertEqual(results[NUM_ROWS], self.NUM_ROWS)
        self.assertTrue(NUM_COLUMNS in results.keys())
        self.assertEqual(results[NUM_COLUMNS], self.NUM_COLS)

    def test_GET_info_cardinality(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_INFO))
        self.assertTrue(isinstance(results, dict))
        self.assertTrue(SCHEMA in results.keys())
        schema = results[SCHEMA]
        for key, column in schema.items():
            if column[Dataset.OLAP_TYPE] == DIMENSION:
                self.assertTrue(Dataset.CARDINALITY in column.keys())
                self.assertEqual(
                    column[Dataset.CARDINALITY], self.cardinalities[key])
            else:
                self.assertFalse(Dataset.CARDINALITY in column.keys())

    def test_GET_info_after_row_update(self):
        self._post_file()
        self._post_row_updates()
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_INFO))
        self.assertEqual(results[NUM_ROWS], self.NUM_ROWS + 1)

    def test_GET_info_after_adding_calculations(self):
        self._post_file()
        self._post_calculations(formulae=self.default_formulae)
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_INFO))
        self.assertEqual(results[NUM_COLUMNS], self.NUM_COLS +
                         len(self.default_formulae))

    def test_GET_schema(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_INFO))
        self.assertTrue(isinstance(results, dict))
        result_keys = results.keys()
        for key in [Dataset.CREATED_AT, ID, SCHEMA, Dataset.UPDATED_AT]:
            self.assertTrue(key in result_keys)
        self.assertEqual(results[SCHEMA]['submit_date'][SIMPLETYPE], DATETIME)

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
        self._test_get_with_query_or_select('{"rating": "delectible"}',
                                            num_results=11)

    def test_GET_with_bad_query(self):
        self._post_file()
        results = json.loads(self.controller.GET(self.dataset_id,
                             query='bad json'))
        self.assertTrue('JSON' in results[ERROR])

    def test_GET_with_date_query(self):
        query = {
            'submit_date': {'$lt': mktime(datetime.now().timetuple())}
        }
        self._test_get_with_query_or_select(
            query=json.dumps(query),
            num_results=self.NUM_ROWS)
        query = {
            'submit_date': {'$gt': mktime(datetime.now().timetuple())}
        }
        self._test_get_with_query_or_select(
            query=json.dumps(query),
            num_results=0)
        date = mktime(datetime(2012, 2, 1, 0).timetuple())
        query = {
            'submit_date': {'$gt': date}
        }
        self._test_get_with_query_or_select(
            query=json.dumps(query),
            num_results=4)

    def test_GET_with_select(self):
        self._test_get_with_query_or_select(select='{"rating": 1}',
                                            result_keys=['rating'])

    def test_GET_with_select_and_query(self):
        self._test_get_with_query_or_select('{"rating": "delectible"}',
                                            '{"rating": 1}',
                                            num_results=11,
                                            result_keys=['rating'])

    def test_GET_summary(self):
        self._post_file()
        results = self.controller.GET(
            self.dataset_id, mode=Datasets.MODE_SUMMARY,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)
        results = self._test_summary_results(results)
        self._test_summary_no_group(results)

    def test_GET_summary_no_select(self):
        self._post_file()
        results = self.controller.GET(
            self.dataset_id, mode=Datasets.MODE_SUMMARY)
        results = json.loads(results)
        self.assertTrue(ERROR in results.keys())

    def test_GET_summary_with_query(self):
        self._post_file()
        # (sic)
        query_column = 'rating'
        results = self.controller.GET(
            self.dataset_id,
            mode=Datasets.MODE_SUMMARY,
            query='{"%s": "delectible"}' % query_column,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)
        results = self._test_summary_results(results)
        # ensure only returned results for this query column
        self.assertEqual(len(results[query_column][SUMMARY].keys()), 1)
        self._test_summary_no_group(results)

    def test_GET_summary_with_group(self):
        self._post_file()
        groups = [
            ('rating', ['delectible', 'epic_eat']),
            ('amount', []),
        ]

        for group, column_values in groups:
            json_results = self.controller.GET(
                self.dataset_id,
                mode=Datasets.MODE_SUMMARY,
                group=group,
                select=self.controller.SELECT_ALL_FOR_SUMMARY)
            results = self._test_summary_results(json_results)
            result_keys = results.keys()

            if len(column_values):
                self.assertTrue(group in result_keys, 'group: %s in: %s'
                                % (group, result_keys))
                self.assertEqual(column_values, results[group].keys())
                for column_value in column_values:
                    self._test_summary_no_group(
                        results[group][column_value], group)
            else:
                self.assertFalse(group in results.keys())
                self.assertTrue(ERROR in results.keys())

    def test_GET_summary_with_group_select(self):
        self._post_file()
        group = 'food_type'
        json_select = {'rating': 1}
        json_results = self.controller.GET(
            self.dataset_id,
            mode=Datasets.MODE_SUMMARY,
            group=group,
            select=json.dumps(json_select))
        results = self._test_summary_results(json_results)
        self.assertTrue(group in results.keys())
        for summary in results[group].values():
            self.assertTrue(len(summary.keys()), 1)

    def test_GET_summary_with_multigroup(self):
        self._post_file()
        group_columns = 'rating,food_type'
        results = self.controller.GET(
            self.dataset_id,
            mode=Datasets.MODE_SUMMARY,
            group=group_columns,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)
        results = self._test_summary_results(results)
        self.assertFalse(ERROR in results.keys())
        self.assertTrue(group_columns in results.keys())
        self.assertEqual(
            len(results[group_columns].keys()[0].split(GROUP_DELIMITER)),
            len(group_columns.split(GROUP_DELIMITER)))

    def test_GET_summary_multigroup_noncat_group(self):
        self._post_file()
        group_columns = 'rating,amount'
        results = self.controller.GET(
            self.dataset_id,
            mode=Datasets.MODE_SUMMARY,
            group=group_columns,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)
        results = self._test_summary_results(results)
        self.assertTrue(ERROR in results.keys())

    def test_GET_summary_nonexistent_group(self):
        self._post_file()
        group_columns = 'bongo'
        results = self.controller.GET(
            self.dataset_id,
            mode=Datasets.MODE_SUMMARY,
            group=group_columns,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)
        results = self._test_summary_results(results)
        self.assertTrue(ERROR in results.keys())

    def test_GET_summary_with_group_and_query(self):
        self._post_file()
        query_column = 'rating'
        results = self.controller.GET(
            self.dataset_id,
            mode=Datasets.MODE_SUMMARY,
            group='rating',
            query='{"%s": "delectible"}' % query_column,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)
        results = self._test_summary_results(results)
        self.assertEqual(len(results[query_column].keys()), 1)

    def test_GET_related_datasets_empty(self):
        self._post_file()
        self._post_calculations(formulae=self.default_formulae)
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_RELATED))
        self.assertTrue(isinstance(results, dict))
        self.assertEqual(len(results.keys()), 0)

    def test_GET_related_datasets(self):
        self._post_file()
        self._post_calculations(
            formulae=self.default_formulae + ['sum(amount)'])
        results = self._test_mode_related()
        row_keys = ['sum_amount_']
        for row in results:
            self.assertEqual(row.keys(), row_keys)
            self.assertTrue(isinstance(row.values()[0], float))

    def test_GET_related_datasets_with_group(self):
        self._post_file()
        group = 'food_type'
        self._post_calculations(self.default_formulae + ['sum(amount)'], group)
        results = self._test_mode_related([group])
        row_keys = [group, 'sum_amount_']
        for row in results:
            self.assertEqual(row.keys(), row_keys)
            self.assertTrue(isinstance(row.values()[0], basestring))
            self.assertTrue(isinstance(row.values()[1], float))

    def test_GET_related_datasets_with_multigroup(self):
        self._post_file()
        group = 'food_type,rating'
        self._post_calculations(self.default_formulae + ['sum(amount)'], group)
        results = self._test_mode_related([group])
        row_keys = (group.split(GROUP_DELIMITER) +
                    ['sum_amount_']).sort()
        for row in results:
            sorted_row_keys = row.keys().sort()
            self.assertEqual(sorted_row_keys, row_keys)
            self.assertTrue(isinstance(row.values()[0], basestring))
            self.assertTrue(isinstance(row.values()[1], basestring))
            self.assertTrue(isinstance(row.values()[2], float))

    def test_GET_related_datasets_with_group_two_calculations(self):
        self._post_file()
        group = 'food_type'
        self._post_calculations(
            self.default_formulae + ['sum(amount)', 'sum(gps_alt)'], group)
        results = self._test_mode_related([group])
        row_keys = [group, 'sum_amount_', 'sum_gps_alt_']
        for row in results:
            self.assertEqual(row.keys(), row_keys)
            self.assertTrue(isinstance(row.values()[0], basestring))
            for value in row.values()[1:]:
                self.assertTrue(isinstance(value, float) or value == 'null')

    def test_GET_related_datasets_with_two_groups(self):
        self._post_file()
        group = 'food_type'
        self._post_calculations(self.default_formulae + ['sum(amount)'])
        self._post_calculations(['sum(gps_alt)'], group)
        groups = ['', group]
        results = self._test_mode_related(groups)
        for row in results:
            self.assertEqual(row.keys(), ['sum_amount_'])
            self.assertTrue(isinstance(row.values()[0], float))

        # get second linked dataset
        results = json.loads(self.controller.GET(self.dataset_id,
                             mode=Datasets.MODE_RELATED))
        self.assertEqual(len(results.keys()), len(groups))
        self.assertEqual(results.keys(), groups)
        linked_dataset_id = results[group]
        self.assertTrue(isinstance(linked_dataset_id, basestring))

        # inspect linked dataset
        results = json.loads(self.controller.GET(linked_dataset_id))
        row_keys = [group, 'sum_gps_alt_']
        for row in results:
            self.assertEqual(row.keys(), row_keys)

    def test_DELETE(self):
        self._post_file()
        result = json.loads(self.controller.DELETE(self.dataset_id))
        self.assertTrue(AbstractController.SUCCESS in result)
        self.assertEqual(
            result[AbstractController.SUCCESS],
            'deleted dataset: %s' % self.dataset_id)

    def test_DELETE_bad_id(self):
        for dataset_name in self.TEST_DATASETS:
            result = json.loads(self.controller.DELETE(
                                self.test_dataset_ids[dataset_name]))
            self.assertTrue(ERROR in result)
