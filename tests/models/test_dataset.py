from pymongo.cursor import Cursor

from tests.test_base import TestBase
from models.dataset import Dataset
from lib.constants import BAMBOO_HASH


class TestDataset(TestBase):

    def test_save(self):
        record = Dataset.save(self.digest)
        self.assertTrue(isinstance(record, dict))
        self.assertTrue('_id' in record.keys())

    def test_find(self):
        record = Dataset.save(self.digest)
        cursor = Dataset.find(self.digest)
        rows = [x for x in cursor]
        self.assertTrue(isinstance(cursor, Cursor))
        self.assertEqual(record, rows[0])
        self.assertEqual(record, Dataset.find_one(self.digest))

    def test_delete(self):
        record = Dataset.save(self.digest)
        records = [x for x in Dataset.find(self.digest)]
        self.assertNotEqual(records, [])
        Dataset.delete(self.digest)
        records = [x for x in Dataset.find(self.digest)]
        self.assertEqual(records, [])
