from time import mktime, sleep

import simplejson as json

from bamboo.controllers.datasets import Datasets
from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets
from bamboo.tests.decorators import requires_async, requires_internet


class TestDatasetsPostUpdate(TestAbstractDatasets):

    def setUp(self):
        TestAbstractDatasets.setUp(self)
        self._file_name_with_slashes = 'good_eats_with_slashes.csv'

    def test_dataset_id_update_bad_dataset_id(self):
        result = json.loads(self.controller.update(dataset_id=111, 
                                                   update=None))
        assert(Datasets.ERROR in result)

    @requires_async
    def test_dataset_update_pending(self):
        self._post_file(self._file_name_with_slashes)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertEqual(dataset.status, Dataset.STATE_PENDING)
        self._put_row_updates()

        while True:
            results = json.loads(self.controller.show(self.dataset_id))

            # wait for the update to finish, or loop forever...
            if len(results) > 19:
                break
            sleep(self.SLEEP_DELAY)

        results = json.loads(self.controller.show(self.dataset_id))
        num_rows_after_update = len(results)
        self.assertEqual(num_rows_after_update, self.NUM_ROWS + 1)

    def test_dataset_update(self):
        self._post_file(self._file_name_with_slashes)
        self._post_calculations(self.default_formulae)
        self._put_row_updates()
        results = json.loads(self.controller.show(self.dataset_id))
        num_rows_after_update = len(results)
        self.assertEqual(num_rows_after_update, self.NUM_ROWS + 1)

        for result in results:
            for column in self.schema.keys():
                self.assertTrue(
                    column in result.keys(),
                    "column %s not in %s" % (column, result.keys()))

        # ensure new row is in results
        self.assertTrue(self._update_values in results)

    def test_dataset_update_with_slugs(self):
        self._post_file(self._file_name_with_slashes)
        self._post_calculations(self.default_formulae)
        self._put_row_updates(file_name='good_eats_update_slugs.json')
        results = json.loads(self.controller.show(self.dataset_id))
        num_rows_after_update = len(results)
        self.assertEqual(num_rows_after_update, self.NUM_ROWS + 1)
        [v['label'] for v in self.schema.values()]

        for result in results:
            for column in self.schema.keys():
                self.assertTrue(
                    column in result.keys(),
                    "column %s not in %s" % (column, result.keys()))

        # ensure new row is in results
        self.assertTrue(self._update_values in results)

    def test_update_multiple(self):
        self._post_file(self._file_name_with_slashes)
        num_rows = len(json.loads(self.controller.show(self.dataset_id)))
        num_update_rows = 2
        self._put_row_updates(file_name='good_eats_update_multiple.json')
        results = json.loads(self.controller.show(self.dataset_id))
        num_rows_after_update = len(results)
        self.assertEqual(num_rows_after_update, num_rows + num_update_rows)

    def test_update_with_aggregation(self):
        self._post_file()
        self._post_calculations(
            formulae=self.default_formulae + ['sum(amount)'])
        num_rows = len(json.loads(self.controller.show(self.dataset_id)))
        self._put_row_updates()
        results = json.loads(self.controller.show(self.dataset_id))
        num_rows_after_update = len(results)
        self.assertEqual(num_rows_after_update, num_rows + 1)

        for result in results:
            for column in self.schema.keys():
                self.assertTrue(
                    column in result.keys(),
                    "column %s not in %s" % (column, result.keys()))

        self._test_aggregations()
