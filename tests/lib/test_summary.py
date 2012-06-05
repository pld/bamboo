from lib.constants import STATS
from lib.summary import summarize_df
from lib.tasks.summarize import summarize
from models.dataset import Dataset
from models.observation import Observation
from tests.test_base import TestBase


class TestSummary(TestBase):

    def test_summarize_df(self):
        result = summarize_df(self.data)
        for summary in result:
            data = summary['data']
            name = summary['name']
            if name == 'food_type':
                self.assertEqual(data['caffeination'], 1)
            elif name == 'amount':
                self.assertEqual(data['count'], 19.0)

    def _test_summarize(self, delay=False):
        dataset = Dataset.save(self.dataset_id)
        Observation.save(self.data, dataset)
        self.assertFalse(STATS in dataset)
        stats = None
        if delay:
            task = summarize.delay(dataset, {}, None, None)
            self.assertTrue(task.ready())
            self.assertTrue(task.successful())
        else:
            stats = summarize(dataset, {}, None, None)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(STATS in dataset)
        if stats:
            self.assertEqual(stats, dataset[STATS])

    def test_summarize(self):
        self._test_summarize()

    def test_summarize_delay(self):
        self._test_summarize(delay=True)
