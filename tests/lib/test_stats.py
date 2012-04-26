from tests.test_base import TestBase
from lib import stats


class TestStats(TestBase):

    def test_summarize_df(self):
        res = stats.summarize_df(self.data)
        self.assertEqual(res[0]['data']['caffeination'], 1)
        self.assertEqual(res[1]['data']['count'], 19.0)
