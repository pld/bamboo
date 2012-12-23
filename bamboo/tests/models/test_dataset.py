from datetime import datetime
import re

from pandas import DataFrame
from pymongo.cursor import Cursor

from bamboo.tests.test_base import TestBase
from bamboo.models.dataset import Dataset
from bamboo.models.observation import Observation
from bamboo.lib.datetools import recognize_dates
from bamboo.lib.mongo import MONGO_RESERVED_KEY_STRS
from bamboo.lib.schema_builder import OLAP_TYPE, SIMPLETYPE


class TestDataset(TestBase):

    def test_save(self):
        for dataset_name in self.TEST_DATASETS:
            record = Dataset().save(self.test_dataset_ids[dataset_name])
            self.assertTrue(isinstance(record, dict))
            self.assertTrue('_id' in record.keys())

    def test_find(self):
        for dataset_name in self.TEST_DATASETS:
            record = Dataset().save(self.test_dataset_ids[dataset_name])
            rows = Dataset.find(self.test_dataset_ids[dataset_name])
            self.assertEqual(record, rows[0].record)
            self.assertEqual(record, Dataset.find_one(
                             self.test_dataset_ids[dataset_name]).record)

    def test_create(self):
        for dataset_name in self.TEST_DATASETS:
            dataset = Dataset().save(self.test_dataset_ids[dataset_name])
            self.assertTrue(isinstance(dataset, dict))

    def test_delete(self):
        for dataset_name in self.TEST_DATASETS:
            record = Dataset()
            record.save(self.test_dataset_ids[dataset_name])
            records = Dataset.find(self.test_dataset_ids[dataset_name])
            self.assertNotEqual(records, [])
            record.delete()
            records = Dataset.find(self.test_dataset_ids[dataset_name])
            self.assertEqual(records, [])

    def test_update(self):
        for dataset_name in self.TEST_DATASETS:
            dataset = Dataset()
            dataset.save(self.test_dataset_ids[dataset_name])
            self.assertFalse('field' in dataset.record)
            dataset.update({'field': {'key': 'value'}})
            dataset = Dataset.find_one(self.test_dataset_ids[dataset_name])
            self.assertTrue('field' in dataset.record)
            self.assertEqual(dataset.record['field'], {'key': 'value'})

    def test_build_schema(self):
        illegal_col_regex = re.compile(r'\W')

        for dataset_name in self.TEST_DATASETS:
            dataset = Dataset()
            dataset.save(self.test_dataset_ids[dataset_name])
            dataset.build_schema(self.get_data(dataset_name))

            # get dataset with new schema
            dataset = Dataset.find_one(self.test_dataset_ids[dataset_name])

            for key in [
                    Dataset.CREATED_AT, Dataset.SCHEMA, Dataset.UPDATED_AT]:
                self.assertTrue(key in dataset.record.keys())

            df_columns = self.get_data(dataset_name).columns.tolist()
            seen_columns = []

            for column_name, column_attributes in dataset.schema.items():
                # check column_name is unique
                self.assertFalse(column_name in seen_columns)
                seen_columns.append(column_name)

                # check column name is only legal chars
                self.assertFalse(illegal_col_regex.search(column_name))

                # check has require attributes
                self.assertTrue(SIMPLETYPE in column_attributes)
                self.assertTrue(OLAP_TYPE in column_attributes)
                self.assertTrue(Dataset.LABEL in column_attributes)

                # check label is an original column
                self.assertTrue(column_attributes[Dataset.LABEL] in df_columns)
                df_columns.remove(column_attributes[Dataset.LABEL])

                # check not reserved key
                self.assertFalse(column_name in MONGO_RESERVED_KEY_STRS)

            # ensure all columns in df_columns have store columns
            self.assertTrue(len(df_columns) == 0)

    def test_dframe(self):
        dataset = Dataset()
        dataset.save(self.test_dataset_ids['good_eats.csv'])
        dataset.save_observations(
            recognize_dates(self.get_data('good_eats.csv')))
        records = [x for x in Observation.find(dataset)]
        dframe = dataset.dframe()

        self.assertTrue(isinstance(dframe, DataFrame))
        self.assertTrue(all(self.get_data('good_eats.csv').reindex(
                        columns=dframe.columns).eq(dframe)))
        columns = dframe.columns
        # ensure no reserved keys
        for key in MONGO_RESERVED_KEY_STRS:
            self.assertFalse(key in columns)
        # ensure date is converted
        self.assertTrue(isinstance(dframe.submit_date[0], datetime))
