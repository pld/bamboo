import simplejson as json

from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets
from bamboo.tests.mock import MockUploadedFile


class TestDatasetsFromSchema(TestAbstractDatasets):

    def setUp(self):
        TestAbstractDatasets.setUp(self)
        result = self._upload_from_schema('good_eats.schema.json')
        self._test_summary_built(result)

    def _upload_from_schema(self, file_name):
        schema = open('tests/fixtures/%s' % file_name)
        mock_uploaded_file = MockUploadedFile(schema)

        return json.loads(self.controller.create(schema=mock_uploaded_file))

    def test_create_from_schema(self):
        results = json.loads(self.controller.show(self.dataset_id))

        self.assertTrue(isinstance(results, list))
        self.assertEqual(len(results), 0)

        results = json.loads(self.controller.info(self.dataset_id))

        self.assertTrue(isinstance(results, dict))
        self.assertTrue(Dataset.SCHEMA in results.keys())
        self.assertTrue(Dataset.NUM_ROWS in results.keys())
        self.assertEqual(results[Dataset.NUM_ROWS], 0)
        self.assertTrue(Dataset.NUM_COLUMNS in results.keys())
        self.assertEqual(results[Dataset.NUM_COLUMNS], self.NUM_COLS)

    def test_create_from_schema_and_update(self):
        results = json.loads(self.controller.show(self.dataset_id))

        self.assertFalse(len(results))

        dataset = Dataset.find_one(self.dataset_id)

        self.assertEqual(dataset.num_rows, 0)

        old_schema = dataset.schema
        self._put_row_updates()
        results = json.loads(self.controller.show(self.dataset_id))

        self.assertTrue(len(results))

        for result in results:
            self.assertTrue(isinstance(result, dict))
            self.assertTrue(len(result.keys()))

        dataset = Dataset.find_one(self.dataset_id)

        self.assertEqual(dataset.num_rows, 1)

        new_schema = dataset.schema

        self.assertEqual(set(old_schema.keys()), set(new_schema.keys()))
        for column in new_schema.keys():
            if new_schema.cardinality(column):
                self.assertEqual(new_schema.cardinality(column), 1)

    def test_create_one_from_schema_and_join(self):
        left_dataset_id = self.dataset_id
        right_dataset_id = self._post_file('good_eats_aux.csv')

        on = 'food_type'
        dataset_id_tuples = [
            (left_dataset_id, right_dataset_id),
            (right_dataset_id, left_dataset_id),
        ]

        for dataset_ids in dataset_id_tuples:
            result = json.loads(self.controller.join(*dataset_ids, on=on))
            expected_schema_keys = set(sum([
                Dataset.find_one(dataset_id).schema.keys()
                for dataset_id in dataset_ids], []))

            self.assertTrue(isinstance(result, dict))
            self.assertTrue(Dataset.ID in result)
            merge_dataset_id = result[Dataset.ID]
            dataset = Dataset.find_one(merge_dataset_id)
            self.assertEqual(dataset.num_rows, 0)
            self.assertEqual(dataset.num_columns, len(expected_schema_keys))
            schema_keys = set(dataset.schema.keys())
            self.assertEqual(schema_keys, expected_schema_keys)

    def test_create_two_from_schema_and_join(self):
        left_dataset_id = self.dataset_id

        schema = open('tests/fixtures/good_eats_aux.schema.json')
        mock_uploaded_file = MockUploadedFile(schema)
        result = json.loads(
            self.controller.create(schema=mock_uploaded_file))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Dataset.ID in result)
        right_dataset_id = result[Dataset.ID]

        on = 'food_type'
        dataset_id_tuples = [
            (left_dataset_id, right_dataset_id),
            (right_dataset_id, left_dataset_id),
        ]

        for dataset_ids in dataset_id_tuples:
            result = json.loads(self.controller.join(*dataset_ids, on=on))
            expected_schema_keys = set(sum([
                Dataset.find_one(dataset_id).schema.keys()
                for dataset_id in dataset_ids], []))

            self.assertTrue(isinstance(result, dict))
            self.assertTrue(Dataset.ID in result)
            merge_dataset_id = result[Dataset.ID]
            dataset = Dataset.find_one(merge_dataset_id)
            self.assertEqual(dataset.num_rows, 0)
            self.assertEqual(dataset.num_columns, len(expected_schema_keys))
            schema_keys = set(dataset.schema.keys())
            self.assertEqual(schema_keys, expected_schema_keys)

    def test_create_two_from_schema_and_join_and_update_vacuous_rhs(self):
        left_dataset_id = self.dataset_id

        schema = open('tests/fixtures/good_eats_aux.schema.json')
        mock_uploaded_file = MockUploadedFile(schema)
        result = json.loads(
            self.controller.create(schema=mock_uploaded_file))
        right_dataset_id = result[Dataset.ID]

        on = 'food_type'
        result = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))
        merged_dataset_id = self.dataset_id = result[Dataset.ID]

        num_rows = 0
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

        num_rows += 1
        self._put_row_updates()
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

        num_rows += 1
        self._put_row_updates(left_dataset_id)
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

        self._put_row_updates(right_dataset_id)
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

    def test_create_two_from_schema_and_join_and_update_child_rhs(self):
        left_dataset_id = self.dataset_id

        result = self._upload_from_schema('good_eats_aux.schema.json')
        right_dataset_id = result[Dataset.ID]

        on = 'food_type'
        result = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))
        merged_dataset_id = self.dataset_id = result[Dataset.ID]

        num_rows = 0
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

        num_rows += 1
        self._put_row_updates(file_name='good_eats_update_bg.json')
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

        self._put_row_updates(right_dataset_id,
                              file_name='good_eats_aux_update.json')
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

    def test_create_two_from_schema_and_join_and_update_lhs_rhs(self):
        left_dataset_id = self.dataset_id

        result = self._upload_from_schema('good_eats_aux.schema.json')
        right_dataset_id = result[Dataset.ID]

        on = 'food_type'
        result = json.loads(self.controller.join(
            left_dataset_id, right_dataset_id, on=on))
        merged_dataset_id = self.dataset_id = result[Dataset.ID]

        num_rows = 0
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

        num_rows += 1
        self._put_row_updates(left_dataset_id,
                              file_name='good_eats_update_bg.json')
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))

        prev_num_cols = len(
            Dataset.find_one(merged_dataset_id).dframe().columns)
        self.dataset_id = right_dataset_id
        self._put_row_updates(right_dataset_id,
                              file_name='good_eats_aux_update.json')
        results = json.loads(self.controller.show(merged_dataset_id))
        self.assertEqual(num_rows, len(results))
        result = results[0]
        self.assertTrue('code' in result.keys())
        self.assertFalse(result['code'] is None)
