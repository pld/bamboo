from datetime import datetime

from pandas import Series

from bamboo.core.frame import BAMBOO_RESERVED_KEYS, BambooFrame,\
    PARENT_DATASET_ID
from bamboo.lib.schema_builder import DATETIME, SIMPLETYPE, Schema
from bamboo.lib.mongo import MONGO_RESERVED_KEYS
from bamboo.tests.test_base import TestBase


class TestFrame(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.get_data('good_eats.csv')
        self.bframe = BambooFrame(self.dframe)

    def _add_bamboo_reserved_keys(self, value=1):
        for key in BAMBOO_RESERVED_KEYS:
            column = Series([value] * len(self.bframe))
            column.name = key
            self.bframe = BambooFrame(self.bframe.join(column))

    def test_add_parent_column(self):
        value = 1
        self._add_bamboo_reserved_keys(value)
        for index, item in self.bframe[PARENT_DATASET_ID].iteritems():
            self.assertEqual(item, value)

    def test_decode_mongo_reserved_keys(self):
        prev_columns = self.bframe.columns
        for col in MONGO_RESERVED_KEYS:
            self.assertTrue(col in self.bframe.columns)
        self.bframe.decode_mongo_reserved_keys()
        for col in MONGO_RESERVED_KEYS:
            self.assertFalse(col in self.bframe.columns)

    def test_recognize_dates(self):
        bframe_with_dates = self.bframe.recognize_dates()
        for field in bframe_with_dates['submit_date']:
            self.assertTrue(isinstance(field, datetime))

    def test_recognize_dates_from_schema(self):
        schema = Schema({
            'submit_date': {
                SIMPLETYPE: DATETIME
            }
        })
        bframe_with_dates = self.bframe.recognize_dates_from_schema(schema)
        for field in bframe_with_dates['submit_date']:
            self.assertTrue(isinstance(field, datetime))

    def test_remove_bamboo_reserved_keys(self):
        self._add_bamboo_reserved_keys()
        for key in BAMBOO_RESERVED_KEYS:
            self.assertTrue(key in self.bframe.columns)
        self.bframe.remove_bamboo_reserved_keys()
        for key in BAMBOO_RESERVED_KEYS:
            self.assertFalse(key in self.bframe.columns)

    def test_remove_bamboo_reserved_keys_not_parent(self):
        self._add_bamboo_reserved_keys()
        for key in BAMBOO_RESERVED_KEYS:
            self.assertTrue(key in self.bframe.columns)
        self.bframe.remove_bamboo_reserved_keys(True)
        for key in BAMBOO_RESERVED_KEYS:
            if key == PARENT_DATASET_ID:
                self.assertTrue(key in self.bframe.columns)
            else:
                self.assertFalse(key in self.bframe.columns)

    def test_only_rows_for_parent_id(self):
        parent_id = 1
        len_parent_rows = len(self.bframe) / 2

        column = Series([parent_id] * len_parent_rows)
        column.name = PARENT_DATASET_ID

        self.bframe = BambooFrame(self.bframe.join(column))
        bframe_only = self.bframe.only_rows_for_parent_id(parent_id)

        self.assertFalse(PARENT_DATASET_ID in bframe_only.columns)
        self.assertEqual(len(bframe_only), len_parent_rows)

    def test_to_jsondict(self):
        jsondict = self.bframe.to_jsondict()
        self.assertEqual(len(jsondict), len(self.bframe))
        for col in jsondict:
            self.assertEqual(len(col), len(self.bframe.columns))

    def test_to_json(self):
        json = self.bframe.to_json()
        self.assertEqual(type(json), str)
