from numpy import isnan

from bamboo.core.frame import RESERVED_KEYS
from bamboo.lib.schema_builder import CARDINALITY, Schema, schema_from_dframe
from bamboo.tests.test_base import TestBase


class TestSchema(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.get_data('good_eats.csv')

    def test_init(self):
        schema = Schema()
        self.assertTrue(isinstance(schema, dict))

    def test_rebuild(self):
        schema = Schema()
        new_schema = schema.rebuild(self.dframe)

        self.assertNotEqual(schema, new_schema)

    def test_rebuild_merge(self):
        col = 'not-in-dframe'
        schema = Schema({col: {}})
        new_schema = schema.rebuild(self.dframe)

        self.assertEqual(new_schema[col], {})

    def test_rebuild_no_merge(self):
        col = 'not-in-dframe'
        schema = Schema({col: {}})
        new_schema = schema.rebuild(self.dframe, overwrite=True)

        self.assertFalse(col in new_schema)

    def test_schema_from_dframe_unique_encoded_columns(self):
        self.dframe.rename(columns={'food_type': 'rating+',
                                    'comments': 'rating-'}, inplace=True)
        schema = schema_from_dframe(self.dframe)

        self.assertTrue('rating_' in schema)
        self.assertTrue('rating__' in schema)

    def test_schema_from_dframe_cardnalities(self):
        schema = schema_from_dframe(self.dframe)

        for column, column_schema in schema.items():
            card = column_schema[CARDINALITY]
            self.assertTrue(card <= len(self.dframe))
            self.assertTrue(card >= 0)

            if card == 0:
                self.assertTrue(all([isnan(x) for x in self.dframe[column]]))

    def test_schema_from_dframe_no_reserved_keys(self):
        for key in RESERVED_KEYS:
            self.dframe[key] = 1

        for key in RESERVED_KEYS:
            self.assertTrue(key in self.dframe.columns)

        schema = schema_from_dframe(self.dframe)

        for key in RESERVED_KEYS:
            self.assertFalse(key in schema)
