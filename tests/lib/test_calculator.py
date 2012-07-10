from tests.test_base import TestBase

from models.dataset import Dataset
from models.observation import Observation


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.test_dataset_ids['good_eats.csv'])
