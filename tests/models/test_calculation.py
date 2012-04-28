from pymongo.cursor import Cursor

from models.calculation import Calculation
from models.dataset import Dataset
from tests.test_base import TestBase


class TestCalculation(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.dataset_id)
        self.formula = 'x + y'
        self.name = 'test'

    def test_save(self):
        record = Calculation.save(self.dataset, self.formula, self.name)
        self.assertTrue(isinstance(record, dict))
        self.assertTrue('_id' in record.keys())

    def test_find(self):
        record = Calculation.save(self.dataset, self.formula, self.name)
        cursor = Calculation.find(self.dataset)
        rows = [x for x in cursor]
        self.assertTrue(isinstance(cursor, Cursor))
        self.assertEqual(record, rows[0])
