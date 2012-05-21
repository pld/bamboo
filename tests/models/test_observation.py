from pandas import DataFrame, read_csv
from pymongo.cursor import Cursor

from lib.constants import MONGO_RESERVED_KEYS
from lib.mongo import _prefix_mongo_reserved_key, mongo_decode_keys
from lib.io import open_data_file
from models.dataset import Dataset
from models.observation import Observation
from tests.test_base import TestBase


class TestObservation(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.dataset_id)

    def _save_records(self):
        records = Observation.save(self.data, self.dataset)
        cursor = Observation.find(self.dataset)
        records = [x for x in cursor]
        self.assertTrue(isinstance(records, list))
        self.assertTrue(isinstance(records[0], dict))
        self.assertTrue('_id' in records[0].keys())
        return records

    def test_save(self):
        records = self._save_records()
        self.assertEqual(len(records), 19)

    def test_save_over_bulk(self):
        f = open_data_file('file://tests/fixtures/good_eats_large.csv')
        self.data = read_csv(f, na_values=['n/a'])
        records = self._save_records()
        self.assertEqual(len(records), 1001)

    def test_find(self):
        Observation.save(self.data, self.dataset)
        cursor = Observation.find(self.dataset)
        self.assertTrue(isinstance(cursor, Cursor))

    def test_find_as_df(self):
        records = Observation.save(self.data, self.dataset)
        dframe = Observation.find(self.dataset, as_df=True)
        self.assertTrue(isinstance(dframe, DataFrame))
        self.assertEqual(self.data.reindex(columns=dframe.columns), dframe)
        columns = dframe.columns
        for key in MONGO_RESERVED_KEYS:
            self.assertFalse(_prefix_mongo_reserved_key(key) in columns)

    def test_find_with_query(self):
        Observation.save(self.data, self.dataset)
        cursor = Observation.find(self.dataset, '{"rating": "delectible"}')
        self.assertTrue(isinstance(cursor, Cursor))

    def test_find_with_bad_query_json(self):
        Observation.save(self.data, self.dataset)
        cursor = Observation.find(self.dataset, '{rating: "delectible"}')
        self.assertTrue(isinstance(cursor, basestring))

    def test_delete(self):
        Observation.save(self.data, self.dataset)
        records = [x for x in Observation.find(self.dataset)]
        self.assertNotEqual(records, [])
        Observation.delete(self.dataset)
        records = [x for x in Observation.find(self.dataset)]
        self.assertEqual(records, [])
