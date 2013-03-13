from pandas import Series
import simplejson as json

from bamboo.controllers.datasets import Datasets
from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasetsEdits(TestAbstractDatasets):

    def setUp(self):
        TestAbstractDatasets.setUp(self)

    def test_show_row(self):
        dataset_id = self._post_file()
        result = json.loads(self.controller.row_show(dataset_id, 0))

        self.assertTrue(isinstance(result, dict))
        self.assertEqual(9.0, result['amount'])

        result = json.loads(self.controller.row_show(dataset_id, "0"))

        self.assertTrue(isinstance(result, dict))
        self.assertEqual(9.0, result['amount'])

    def test_show_row_nonexistent_index(self):
        dataset_id = self._post_file()
        result = json.loads(self.controller.row_show(dataset_id, "90"))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Datasets.ERROR in result)

    def test_show_row_bad_index(self):
        dataset_id = self._post_file()
        result = json.loads(self.controller.row_show(dataset_id, "A"))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Datasets.ERROR in result)

    def test_delete_row(self):
        dataset_id = self._post_file()
        index = 0
        expected_dframe = Dataset.find_one(
            dataset_id).dframe()[index + 1:].reset_index()
        del expected_dframe['index']

        results = json.loads(self.controller.row_delete(dataset_id, index))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        dataset = Dataset.find_one(dataset_id)
        dframe = dataset.dframe()
        self.assertEqual(self.NUM_ROWS - 1, len(dframe))
        self._check_dframes_are_equal(expected_dframe, dframe)

        # check info updated
        info = dataset.info()
        self.assertEqual(self.NUM_ROWS - 1, info[Dataset.NUM_ROWS])

    def test_update_row(self):
        dataset_id = self._post_file()
        index = 0
        update = {'amount': 10, 'food_type': 'breakfast'}
        expected_dframe = Dataset.find_one(dataset_id).dframe()
        expected_row = expected_dframe.ix[0].to_dict()
        expected_row.update(update)
        expected_dframe.ix[0] = Series(expected_row)

        results = json.loads(self.controller.row_update(dataset_id, index,
                                                        json.dumps(update)))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        dframe = Dataset.find_one(dataset_id).dframe()
        self.assertEqual(self.NUM_ROWS, len(dframe))
        self._check_dframes_are_equal(expected_dframe, dframe)
