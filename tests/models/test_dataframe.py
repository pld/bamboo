from pymongo.cursor import Cursor

from tests.test_base import TestBase
from models.dataset import Dataset
from lib.constants import BAMBOO_HASH


class TestDataset(TestBase):

    def test_save(self):
        record = Dataset.save(self.digest)
        self.assertTrue(record, isinstance(record, dict))

    def test_find(self):
        record = Dataset.save(self.digest)
        cursor = Dataset.find(self.digest)
        rows = [x for x in cursor]
        self.assertTrue(cursor, isinstance(cursor, Cursor))
        self.assertEqual(record, rows[0])
        self.assertEqual(record, Dataset.find_one(self.digest))
