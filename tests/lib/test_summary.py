from lib.summary import summarize_df
from tests.test_base import TestBase


class TestSummary(TestBase):

    def test_summarize_df(self):
        res = summarize_df(self.data)
        for summary in res:
            data = summary['data']
            name = summary['name']
            if name == 'food_type':
                self.assertEqual(data['caffeination'], 1)
            elif name == 'amount':
                self.assertEqual(data['count'], 19.0)
