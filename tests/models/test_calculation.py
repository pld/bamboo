from pymongo.cursor import Cursor

from lib.constants import ERROR
from models.calculation import Calculation
from models.dataset import Dataset
from models.observation import Observation
from tests.test_base import TestBase


class TestCalculation(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.test_dataset_ids['good_eats.csv'])
        self.formula = 'rating'
        self.name = 'test'

    def _save_observations_and_calculation(self, formula=None):
        if not formula:
            formula = self.formula
        Observation.save(self.test_data['good_eats.csv'], self.dataset)
        return Calculation.save(self.dataset, formula, self.name)

    def test_save(self):
        record = self._save_observations_and_calculation()
        self.assertTrue(isinstance(record, dict))
        self.assertTrue(Calculation.FORMULA in record.keys())

    def test_save_improper_formula(self):
        record = self._save_observations_and_calculation('NON_EXISTENT_COLUMN')
        self.assertTrue(isinstance(record, dict))
        self.assertTrue(ERROR in record.keys())
        self.assertTrue('Missing column' in record[ERROR].__str__())

    def test_save_unparsable_formula(self):
        record = self._save_observations_and_calculation(
            '=NON_EXISTENT_COLUMN')
        self.assertTrue(isinstance(record, dict))
        self.assertTrue(ERROR in record.keys())
        self.assertTrue('Parse Failure' in record[ERROR].__str__())

    def test_save_improper_formula_no_data(self):
        record = Calculation.save(self.dataset, 'NON_EXISTENT_COLUMN',
                                  self.name)
        self.assertTrue(isinstance(record, dict))
        self.assertTrue(ERROR in record.keys())
        self.assertTrue('Missing column' in record[ERROR].__str__())

    def test_save_unparsable_formula_no_data(self):
        record = Calculation.save(self.dataset, '=NON_EXISTENT_COLUMN',
                                  self.name)
        self.assertTrue(isinstance(record, dict))
        self.assertTrue(ERROR in record.keys())
        self.assertTrue('Parse Failure' in record[ERROR].__str__())

    def test_find(self):
        record = self._save_observations_and_calculation()
        rows = Calculation.find(self.dataset)
        self.assertEqual(record, rows[0])
