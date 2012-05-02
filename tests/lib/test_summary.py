from tests.test_base import TestBase
from lib.summary import summarize_df


class TestSummary(TestBase):

    def test_summarize_df(self):
        res = summarize_df(self.data)
        self.assertEqual(res[0]['data']['caffeination'], 1)
        self.assertEqual(res[1]['data']['count'], 19.0)
