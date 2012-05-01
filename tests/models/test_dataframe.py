from pymongo.cursor import Cursor

from tests.test_base import TestBase
from models.dataframe import Dataframe
from lib.constants import BAMBOO_HASH
from lib.utils import df_to_hexdigest


class TestDataFrame(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.digest = df_to_hexdigest(self.data)

    def test_dataframe(self):
        record = Dataframe.save(self.digest)
        self.assertTrue(record, isinstance(record, dict))

    def test_find(self):
        record = Dataframe.save(self.digest)
        cursor = Dataframe.find(self.digest)
        rows = [x for x in cursor]
        self.assertTrue(cursor, isinstance(cursor, Cursor))
        self.assertEqual(record, rows[0])
        self.assertEqual(record, Dataframe.find_one(self.digest))
