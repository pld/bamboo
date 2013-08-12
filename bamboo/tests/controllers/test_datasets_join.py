import simplejson as json

from bamboo.controllers.datasets import Datasets
from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasets(TestAbstractDatasets):

    def setUp(self):
        TestAbstractDatasets.setUp(self)

    def test_join_datasets(self):
        left_dataset_id = self._post_file()
        right_dataset_id = self._post_file('good_eats_aux.csv')
        on = 'food_type'
        results = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))

        self.assertTrue(Datasets.SUCCESS in results.keys())
        self.assertTrue(Dataset.ID in results.keys())

        joined_dataset_id = results[Dataset.ID]
        data = json.loads(self.controller.show(joined_dataset_id))

        self.assertTrue('code' in data[0].keys())

        left_dataset = Dataset.find_one(left_dataset_id)
        right_dataset = Dataset.find_one(right_dataset_id)

        self.assertEqual([('right', right_dataset_id, on, joined_dataset_id)],
                         left_dataset.joined_dataset_ids)
        self.assertEqual([('left', left_dataset_id, on, joined_dataset_id)],
                         right_dataset.joined_dataset_ids)

    def test_join_datasets_different_columns(self):
        left_dataset_id = self._post_file()
        right_dataset_id = self._post_file('good_eats_aux_join.csv')
        on_lhs = 'food_type'
        on_rhs = 'also_food_type'
        on = '%s,%s' % (on_lhs, on_rhs)
        results = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))

        self.assertTrue(Datasets.SUCCESS in results.keys())
        self.assertTrue(Dataset.ID in results.keys())

        joined_dataset_id = results[Dataset.ID]
        data = json.loads(self.controller.show(joined_dataset_id))

        self.assertTrue('code' in data[0].keys())

        left_dataset = Dataset.find_one(left_dataset_id)
        right_dataset = Dataset.find_one(right_dataset_id)

        self.assertEqual([('right', right_dataset_id, on, joined_dataset_id)],
                         left_dataset.joined_dataset_ids)
        self.assertEqual([('left', left_dataset_id, on, joined_dataset_id)],
                         right_dataset.joined_dataset_ids)

    def test_join_datasets_non_unique_rhs(self):
        left_dataset_id = self._post_file()
        right_dataset_id = self._post_file()
        results = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on='food_type'))

        self.assertTrue(Datasets.ERROR in results.keys())
        self.assertTrue('right' in results[Datasets.ERROR])
        self.assertTrue('not unique' in results[Datasets.ERROR])

    def test_join_datasets_on_col_not_in_lhs(self):
        left_dataset_id = self._post_file()
        right_dataset_id = self._post_file('good_eats_aux.csv')
        on = 'code'
        results = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))

        self.assertTrue(Datasets.ERROR in results.keys())
        self.assertTrue('left' in results[Datasets.ERROR])

    def test_join_datasets_on_col_not_in_rhs(self):
        left_dataset_id = self._post_file()
        right_dataset_id = self._post_file('good_eats_aux.csv')
        on = 'rating'
        results = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))

        self.assertTrue(Datasets.ERROR in results.keys())
        self.assertTrue('right' in results[Datasets.ERROR])

    def test_join_datasets_overlap(self):
        left_dataset_id = self._post_file('good_eats.csv')
        right_dataset_id = self._post_file('good_eats.csv')
        on = 'food_photo'

        results = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))

        self.assertTrue(Datasets.SUCCESS in results.keys())
        self.assertTrue(Dataset.ID in results.keys())

        joined_dataset_id = results[Dataset.ID]
        data = json.loads(self.controller.show(joined_dataset_id))
        keys = data[0].keys()

        for column in Dataset.find_one(left_dataset_id).dframe().columns:
            if column != on:
                self.assertTrue('%s_x' % column in keys)
                self.assertTrue('%s_y' % column in keys)
