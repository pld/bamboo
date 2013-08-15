from pandas import Series
import simplejson as json

from bamboo.controllers.datasets import Datasets
from bamboo.models.dataset import Dataset
from bamboo.models.observation import Observation
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasetsEdit(TestAbstractDatasets):

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
        dataset = Dataset.find_one(dataset_id)
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

        # check that row is softly deleted
        all_observations = Observation.find(dataset, include_deleted=True)
        self.assertEqual(self.NUM_ROWS, len(all_observations))

    def test_delete_row_with_agg(self):
        amount_sum = 2007.5
        amount_sum_after = 1998.5
        index = 0

        self.dataset_id = self._post_file()
        self._post_calculations(formulae=['sum(amount)'])
        agg = self._test_aggregations()[0]
        self.assertEqual(agg['sum_amount_'], amount_sum)

        results = json.loads(
            self.controller.row_delete(self.dataset_id, index))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        agg = self._test_aggregations()[0]
        self.assertEqual(agg['sum_amount_'], amount_sum_after)

    def test_delete_row_with_join(self):
        index = 0

        left_dataset_id = self._post_file()
        right_dataset_id = self._post_file('good_eats_aux.csv')
        on = 'food_type'
        results = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))
        joined_dataset_id = results[Dataset.ID]

        results = json.loads(self.controller.join(
            joined_dataset_id, right_dataset_id, on=on))
        joined_dataset_id2 = results[Dataset.ID]

        results = json.loads(
            self.controller.row_delete(left_dataset_id, index))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        dframe = Dataset.find_one(joined_dataset_id).dframe(index=True)
        self.assertFalse(index in dframe['index'].tolist())

        dframe = Dataset.find_one(joined_dataset_id2).dframe(index=True)
        self.assertFalse(index in dframe['index'].tolist())

    def test_delete_row_with_merge(self):
        index = 0

        dataset_id1 = self._post_file()
        dataset_id2 = self._post_file()
        result = json.loads(self.controller.merge(
            dataset_ids=json.dumps([dataset_id1, dataset_id2])))
        merged_id = result[Dataset.ID]

        results = json.loads(
            self.controller.row_delete(dataset_id2, index))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        results = json.loads(
            self.controller.row_delete(dataset_id1, index))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        dframe = Dataset.find_one(merged_id).dframe(index=True)
        self.assertFalse(index in dframe['index'].tolist())
        self.assertFalse(index + self.NUM_ROWS in dframe['index'].tolist())

    def test_edit_row(self):
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

        dataset = Dataset.find_one(dataset_id)
        dframe = dataset.dframe()
        self.assertEqual(self.NUM_ROWS, len(dframe))
        self._check_dframes_are_equal(expected_dframe, dframe)

        # check that previous row exists
        all_observations = Observation.find(dataset, include_deleted=True)
        self.assertEqual(self.NUM_ROWS + 1, len(all_observations))

    def test_edit_row_with_calculation(self):
        amount_before = 9
        amount_after = 10
        value = 5
        index = 0
        update = {'amount': amount_after, 'food_type': 'breakfast'}

        self.dataset_id = self._post_file()
        self._post_calculations(formulae=['amount + %s' % value])

        result = json.loads(self.controller.row_show(self.dataset_id, index))
        self.assertEqual(amount_before + value, result['amount___%s' % value])

        results = json.loads(self.controller.row_update(self.dataset_id, index,
                                                        json.dumps(update)))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        result = json.loads(self.controller.row_show(self.dataset_id, index))
        self.assertEqual(amount_after + value, result['amount___%s' % value])

    def test_edit_row_with_agg(self):
        amount_sum = 2007.5
        amount_sum_after = 2008.5

        self.dataset_id = self._post_file()
        self._post_calculations(formulae=['sum(amount)'])
        agg = self._test_aggregations()[0]
        self.assertEqual(agg['sum_amount_'], amount_sum)

        index = 0
        update = {'amount': 10, 'food_type': 'breakfast'}
        results = json.loads(self.controller.row_update(self.dataset_id, index,
                                                        json.dumps(update)))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        agg = self._test_aggregations()[0]
        self.assertEqual(agg['sum_amount_'], amount_sum_after)

    def test_edit_row_with_join(self):
        index = 0
        value = 10
        update = {'amount': value, 'food_type': 'breakfast'}

        left_dataset_id = self._post_file()
        right_dataset_id = self._post_file('good_eats_aux.csv')
        on = 'food_type'
        results = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))
        joined_dataset_id = results[Dataset.ID]

        results = json.loads(self.controller.join(
            joined_dataset_id, right_dataset_id, on=on))
        joined_dataset_id2 = results[Dataset.ID]

        results = json.loads(self.controller.row_update(left_dataset_id, index,
                                                        json.dumps(update)))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        result = json.loads(self.controller.row_show(joined_dataset_id, 0))
        self.assertEqual(value, result['amount'])

        result = json.loads(self.controller.row_show(joined_dataset_id2, 0))
        self.assertEqual(value, result['amount'])

    def test_edit_row_with_join_invalid(self):
        index = 0
        update = {'food_type': 'deserts'}

        left_dataset_id = self._post_file()
        right_dataset_id = self._post_file('good_eats_aux.csv')
        on = 'food_type'
        json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))

        results = json.loads(self.controller.row_update(
            right_dataset_id, index, json.dumps(update)))
        self.assertTrue(Datasets.ERROR in results.keys())

    def test_edit_row_with_merge(self):
        index = 0
        value = 10
        update = {'amount': value, 'food_type': 'breakfast'}

        dataset_id1 = self._post_file()
        dataset_id2 = self._post_file()
        result = json.loads(self.controller.merge(
            dataset_ids=json.dumps([dataset_id1, dataset_id2])))
        merged_id = result[Dataset.ID]

        results = json.loads(self.controller.row_update(dataset_id1, index,
                                                        json.dumps(update)))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        results = json.loads(self.controller.row_update(dataset_id2, index,
                                                        json.dumps(update)))
        self.assertTrue(Datasets.SUCCESS in results.keys())

        result = json.loads(self.controller.row_show(merged_id, index))
        self.assertEqual(value, result['amount'])

        result = json.loads(self.controller.row_show(merged_id, index +
                                                     self.NUM_ROWS))
        self.assertEqual(value, result['amount'])
