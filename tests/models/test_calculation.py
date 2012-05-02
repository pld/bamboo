from models.dataset import Dataset
from models.calculation import Calculation
from tests.test_base import TestBase


class TestCalculation(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.digest)
        self.formula = 'x + y'
        self.name = 'test'

    def test_save(self):
        record = Calculation.save(self.dataset, self.formula, self.name)
        self.assertTrue(isinstance(record, dict))

    def test_find(self):
        pass
