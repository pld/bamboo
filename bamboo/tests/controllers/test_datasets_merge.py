from time import sleep

from pandas import concat
import simplejson as json

from bamboo.core.frame import BAMBOO_RESERVED_KEYS, PARENT_DATASET_ID
from bamboo.lib.mongo import MONGO_RESERVED_KEY_STRS
from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets
from bamboo.tests.decorators import requires_async


class TestDatasetsMerge(TestAbstractDatasets):

    @requires_async
    def test_merge_datasets_0_not_enough(self):
        result = json.loads(self.controller.merge(datasets=json.dumps([])))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(self.controller.ERROR in result)

    @requires_async
    def test_merge_datasets_1_not_enough(self):
        dataset_id = self._post_file()
        result = json.loads(self.controller.merge(
            datasets=json.dumps([dataset_id])))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(self.controller.ERROR in result)

    @requires_async
    def test_merge_datasets_must_exist(self):
        dataset_id = self._post_file()
        result = json.loads(self.controller.merge(
            datasets=json.dumps([dataset_id, 0000])))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(self.controller.ERROR in result)

    def test_merge_datasets(self):
        dataset_id1 = self._post_file()
        dataset_id2 = self._post_file()
        result = json.loads(self.controller.merge(
            datasets=json.dumps([dataset_id1, dataset_id2])))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Dataset.ID in result)

        datasets = [Dataset.find_one(dataset_id)
                    for dataset_id in [dataset_id1, dataset_id2]]

        for dataset in datasets:
            self.assertTrue(result[Dataset.ID] in dataset.merged_dataset_ids)

        dframe1 = datasets[0].dframe()
        merged_dataset = Dataset.find_one(result[Dataset.ID])
        merged_rows = merged_dataset.observations()

        for row in merged_rows:
            self.assertTrue(PARENT_DATASET_ID in row.keys())

        merged_dframe = merged_dataset.dframe()

        self.assertEqual(len(merged_dframe), 2 * len(dframe1))

        expected_dframe = concat([dframe1, dframe1],
                                 ignore_index=True)

        self.assertEqual(list(merged_dframe.columns),
                         list(expected_dframe.columns))

        self._check_dframes_are_equal(merged_dframe, expected_dframe)

    @requires_async
    def test_merge_datasets_async(self):
        dataset_id1 = self._post_file()
        dataset_id2 = self._post_file()

        self.assertEqual(
            Dataset.find_one(dataset_id1).state,
            Dataset.STATE_PENDING)
        self.assertEqual(
            Dataset.find_one(dataset_id2).state,
            Dataset.STATE_PENDING)

        result = json.loads(self.controller.merge(
            datasets=json.dumps([dataset_id1, dataset_id2])))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Dataset.ID in result)

        merged_id = result[Dataset.ID]

        # wait for background tasks for finish
        while True:
            results1 = json.loads(self.controller.show(dataset_id1))
            results2 = json.loads(self.controller.show(dataset_id2))
            results3 = json.loads(self.controller.show(merged_id))

            if all([len(res) for res in [results1, results2, results3]]):
                break

            sleep(self.SLEEP_DELAY)

        while True:
            datasets = [Dataset.find_one(dataset_id)
                        for dataset_id in [dataset_id1, dataset_id2]]

            if all([dataset.is_ready for dataset in datasets]):
                break

            sleep(self.SLEEP_DELAY)

        for dataset in datasets:
            self.assertTrue(merged_id in dataset.merged_dataset_ids)

        dframe1 = datasets[0].dframe()
        merged_dataset = Dataset.find_one(merged_id)
        merged_rows = merged_dataset.observations()

        for row in merged_rows:
            self.assertTrue(PARENT_DATASET_ID in row.keys())

        merged_dframe = merged_dataset.dframe()

        self.assertEqual(len(merged_dframe), 2 * len(dframe1))

        expected_dframe = concat([dframe1, dframe1],
                                 ignore_index=True)

        self.assertEqual(list(merged_dframe.columns),
                         list(expected_dframe.columns))

        self._check_dframes_are_equal(merged_dframe, expected_dframe)

    @requires_async
    def test_merge_datasets_add_calc_async(self):
        dataset_id1 = self._post_file('good_eats_large.csv')
        dataset_id2 = self._post_file('good_eats_large.csv')
        result = json.loads(self.controller.merge(
            datasets=json.dumps([dataset_id1, dataset_id2])))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Dataset.ID in result)

        self.dataset_id = result[Dataset.ID]
        self.schema = json.loads(
            self.controller.info(self.dataset_id))[Dataset.SCHEMA]

        self._post_calculations(['amount < 4'])

    def test_merge_datasets_no_reserved_keys(self):
        dataset_id1 = self._post_file()
        dataset_id2 = self._post_file()
        result = json.loads(self.controller.merge(
            datasets=json.dumps([dataset_id1, dataset_id2])))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Dataset.ID in result)

        response = json.loads(self.controller.show(result[Dataset.ID]))
        row_keys = sum([row.keys() for row in response], [])

        for reserved_key in BAMBOO_RESERVED_KEYS + MONGO_RESERVED_KEY_STRS:
            self.assertFalse(reserved_key in row_keys)
