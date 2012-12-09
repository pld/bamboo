import simplejson as json

from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.core.frame import BambooFrame
from bamboo.lib.io import create_dataset_from_url
from bamboo.lib.jsontools import series_to_jsondict
from bamboo.models.dataset import Dataset
from bamboo.tests.test_base import TestBase


class TestAbstractDatasets(TestBase):

    NUM_COLS = 15
    NUM_ROWS = 19

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Datasets()
        self._file_name = 'good_eats.csv'
        self._update_file_name = 'good_eats_update.json'
        self._update_check_file_path = '%sgood_eats_update_values.json' % (
            self.FIXTURE_PATH)
        self.default_formulae = [
            'amount',
            'amount + 1',
            'amount - 5',
        ]

    def _put_row_updates(self, dataset_id=None, file_name=None, validate=True):
        if not dataset_id:
            dataset_id = self.dataset_id

        if not file_name:
            file_name = self._update_file_name

        update = open('%s%s' % (self.FIXTURE_PATH, file_name), 'r').read()
        result = json.loads(self.controller.update(dataset_id=dataset_id,
                                                   update=update))
        if validate:
            self.assertTrue(isinstance(result, dict))
            self.assertTrue(Dataset.ID in result)

        # set up the (default) values to test against
        with open(self._update_check_file_path, 'r') as f:
            self._update_values = json.loads(f.read())

    def _post_file(self, file_name=None):
        if file_name is None:
            file_name = self._file_name

        self.dataset_id = create_dataset_from_url(
            '%s%s' % (self._local_fixture_prefix(), file_name),
            allow_local_file=True).dataset_id

        self.schema = json.loads(
            self.controller.info(self.dataset_id))[Dataset.SCHEMA]

    def _check_dframes_are_equal(self, dframe1, dframe2):
        self._check_dframe_is_subset(dframe1, dframe2)
        self._check_dframe_is_subset(dframe2, dframe1)

    def _check_dframe_is_subset(self, dframe1, dframe2):
        dframe2_rows = [self._reduce_precision(row) for row in
                        BambooFrame(dframe2).to_jsondict()]

        for row in dframe1.iterrows():
            dframe1_row = self._reduce_precision(series_to_jsondict(row[1]))
            self.assertTrue(dframe1_row in dframe2_rows,
                            'dframe1_row: %s\n\ndframe2_rows: %s' % (
                                dframe1_row, dframe2_rows))

    def _reduce_precision(self, row):
        for key, value in row.iteritems():
            if isinstance(value, float):
                row[key] = round(value, 10)

        return row

    def _post_calculations(self, formulae=[], group=None):
        # must call after _post_file
        controller = Calculations()

        for idx, formula in enumerate(formulae):
            name = 'calc_%d' % idx if not self.schema or\
                formula in self.schema.keys() else formula
            controller.create(self.dataset_id, formula=formula, name=name,
                              group=group)

    def _test_summary_built(self, result):
        # check that summary is created
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Dataset.ID in result)
        self.dataset_id = result[Dataset.ID]

        results = self.controller.summary(
            self.dataset_id,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)

        return self._test_summary_results(results)

    def _test_summary_results(self, results):
        results = json.loads(results)
        self.assertTrue(isinstance(results, dict))
        return results

    def _test_aggregations(self, groups=['']):
        results = json.loads(self.controller.aggregations(self.dataset_id))
        self.assertTrue(isinstance(results, dict))
        self.assertEqual(len(results.keys()), len(groups))
        self.assertEqual(results.keys(), groups)
        linked_dataset_id = results[groups[0]]
        self.assertTrue(isinstance(linked_dataset_id, basestring))

        # inspect linked dataset
        return json.loads(self.controller.show(linked_dataset_id))
