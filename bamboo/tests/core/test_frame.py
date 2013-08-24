from pandas import Series

from bamboo.core.frame import BAMBOO_RESERVED_KEYS, remove_reserved_keys,\
    rows_for_parent_id, PARENT_DATASET_ID
from bamboo.tests.test_base import TestBase


class TestFrame(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.get_data('good_eats.csv')

    def _add_bamboo_reserved_keys(self, value=1):
        for key in BAMBOO_RESERVED_KEYS:
            column = Series([value] * len(self.dframe))
            column.name = key
            self.dframe = self.dframe.join(column)

    def test_add_parent_column(self):
        value = 1
        self._add_bamboo_reserved_keys(value)

        for index, item in self.dframe[PARENT_DATASET_ID].iteritems():
            self.assertEqual(item, value)

    def test_remove_reserved_keys(self):
        self._add_bamboo_reserved_keys()

        for key in BAMBOO_RESERVED_KEYS:
            self.assertTrue(key in self.dframe.columns)

        dframe = remove_reserved_keys(self.dframe)

        for key in BAMBOO_RESERVED_KEYS:
            self.assertFalse(key in dframe.columns)

    def test_remove_reserved_keys_exclusion(self):
        self._add_bamboo_reserved_keys()

        for key in BAMBOO_RESERVED_KEYS:
            self.assertTrue(key in self.dframe.columns)

        dframe = remove_reserved_keys(self.dframe, [PARENT_DATASET_ID])

        for key in BAMBOO_RESERVED_KEYS:
            if key == PARENT_DATASET_ID:
                self.assertTrue(key in dframe.columns)
            else:
                self.assertFalse(key in dframe.columns)

    def test_only_rows_for_parent_id(self):
        parent_id = 1
        len_parent_rows = len(self.dframe) / 2

        column = Series([parent_id] * len_parent_rows)
        column.name = PARENT_DATASET_ID

        self.dframe = self.dframe.join(column)
        dframe_only = rows_for_parent_id(self.dframe, parent_id)

        self.assertFalse(PARENT_DATASET_ID in dframe_only.columns)
        self.assertEqual(len(dframe_only), len_parent_rows)
