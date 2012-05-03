from pandas import DataFrame
from pymongo.cursor import Cursor

from tests.test_base import TestBase
from models.dataset import Dataset
from models.observation import Observation
from lib.constants import DATASET_ID
from lib.utils import mongo_decode_keys


class TestDataset(TestBase):

    def test_save(self):
        record = Dataset.save(self.dataset_id)
        self.assertTrue(isinstance(record, dict))
        self.assertTrue('_id' in record.keys())

    def test_find(self):
        record = Dataset.save(self.dataset_id)
        cursor = Dataset.find(self.dataset_id)
        rows = [x for x in cursor]
        self.assertTrue(isinstance(cursor, Cursor))
        self.assertEqual(record, rows[0])
        self.assertEqual(record, Dataset.find_one(self.dataset_id))

    def test_create(self):
        dataset_id = Dataset.create(self.data)
        self.assertTrue(isinstance(dataset_id, basestring))
        dataset = Dataset.find_one(dataset_id)
        dframe = Observation.find(dataset, as_df=True)
        self.assertTrue(isinstance(dframe, DataFrame))
        self.assertEqual(self.data.reindex(columns=dframe.columns), dframe)

    def test_delete(self):
        record = Dataset.save(self.dataset_id)
        records = [x for x in Dataset.find(self.dataset_id)]
        self.assertNotEqual(records, [])
        Dataset.delete(self.dataset_id)
        records = [x for x in Dataset.find(self.dataset_id)]
        self.assertEqual(records, [])
