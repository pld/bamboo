import re

from pandas import DataFrame
from pymongo.cursor import Cursor

from tests.test_base import TestBase
from models.dataset import Dataset
from models.observation import Observation
from lib.constants import CREATED_AT, DATASET_ID, LABEL, OLAP_TYPE, SCHEMA,\
     SIMPLETYPE, UPDATED_AT
from lib.mongo import mongo_decode_keys


class TestDataset(TestBase):

    def test_save(self):
        for dataset_name in self.TEST_DATASETS:
            record = Dataset.save(self.test_dataset_ids[dataset_name])
            self.assertTrue(isinstance(record, dict))
            self.assertTrue('_id' in record.keys())

    def test_find(self):
        for dataset_name in self.TEST_DATASETS:
            record = Dataset.save(self.test_dataset_ids[dataset_name])
            cursor = Dataset.find(self.test_dataset_ids[dataset_name])
            rows = [x for x in cursor]
            self.assertTrue(isinstance(cursor, Cursor))
            self.assertEqual(record, rows[0])
            self.assertEqual(record, Dataset.find_one(
                        self.test_dataset_ids[dataset_name]))

    def test_create(self):
        for dataset_name in self.TEST_DATASETS:
            dataset = Dataset.create(self.test_dataset_ids[dataset_name])
            self.assertTrue(isinstance(dataset, dict))

    def test_delete(self):
        for dataset_name in self.TEST_DATASETS:
            record = Dataset.save(self.test_dataset_ids[dataset_name])
            records = [x for x in \
                    Dataset.find(self.test_dataset_ids[dataset_name])]
            self.assertNotEqual(records, [])
            Dataset.delete(self.test_dataset_ids[dataset_name])
            records = [x for x in
                    Dataset.find(self.test_dataset_ids[dataset_name])]
            self.assertEqual(records, [])

    def test_update(self):
        for dataset_name in self.TEST_DATASETS:
            dataset = Dataset.create(self.test_dataset_ids[dataset_name])
            self.assertFalse('field' in dataset)
            Dataset.update(dataset, {'field': {'key': 'value'}})
            dataset = Dataset.find_one(self.test_dataset_ids[dataset_name])
            self.assertTrue('field' in dataset)
            self.assertEqual(dataset['field'], {'key': 'value'})

    def test_build_schema(self):
        illegal_col_regex = re.compile(r'\W|[A-Z]')

        for dataset_name in self.TEST_DATASETS:
            dataset = Dataset.create(self.test_dataset_ids[dataset_name])
            Dataset.build_schema(dataset,
                    self.test_data[dataset_name].dtypes)

            # get dataset with new schema
            dataset = Dataset.find_one(self.test_dataset_ids[dataset_name])

            for key in [CREATED_AT, SCHEMA, UPDATED_AT]:
                self.assertTrue(key in dataset.keys())

            df_columns = self.test_data[dataset_name].columns.tolist()
            seen_columns = []

            for column_name, column_attributes in dataset[SCHEMA].items():
                # check column_name is unique
                self.assertFalse(column_name in seen_columns)
                seen_columns.append(column_name)

                # check column name is only legal chars
                self.assertFalse(illegal_col_regex.search(column_name))
                # check has require attributes
                self.assertTrue(SIMPLETYPE in column_attributes)
                self.assertTrue(OLAP_TYPE in column_attributes)
                self.assertTrue(LABEL in column_attributes)

                # check label is an original column
                self.assertTrue(column_attributes[LABEL] in df_columns)
                df_columns.remove(column_attributes[LABEL])

            # ensure all columns in df_columns have store columns
            self.assertTrue(len(df_columns) == 0)
