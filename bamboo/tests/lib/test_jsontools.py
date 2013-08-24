from bamboo.lib.jsontools import df_to_json, df_to_jsondict
from bamboo.tests.test_base import TestBase


class TestFrame(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.get_data('good_eats.csv')

    def test_to_jsondict(self):
        jsondict = df_to_jsondict(self.dframe)
        self.assertEqual(len(jsondict), len(self.dframe))

        for col in jsondict:
            self.assertEqual(len(col), len(self.dframe.columns))

    def test_to_json(self):
        json = df_to_json(self.dframe)
        self.assertEqual(type(json), str)
