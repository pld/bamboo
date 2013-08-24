from bamboo.lib.mongo import df_mongo_decode, MONGO_ID
from bamboo.tests.test_base import TestBase


class TestFrame(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.get_data('good_eats.csv')

    def test_decode_reserved_keys(self):
        self.assertTrue(MONGO_ID in self.dframe.columns)
        dframe = df_mongo_decode(self.dframe)
        self.assertFalse(MONGO_ID in dframe.columns)
