from tests.test_base import TestBase
from models.dataset import Dataset
from models.observation import Observation


class TestObservation(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.digest)

    def test_save(self):
        Observation.save(self.data, self.dataset)

    def test_find(self):
        pass

    def test_delete(self):
        pass
