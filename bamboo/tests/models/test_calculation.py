from nose.tools import assert_raises

from bamboo.core.parser import ParseError
from bamboo.models.calculation import Calculation, DependencyError
from bamboo.models.dataset import Dataset
from bamboo.tests.test_base import TestBase


class TestCalculation(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset()
        self.dataset.save(self.test_dataset_ids['good_eats.csv'])
        self.formula = 'rating'
        self.name = 'test'

    def _save_calculation(self, formula):
        if not formula:
            formula = self.formula
        return self.calculation.save(self.dataset, formula, self.name)

    def _save_observations(self):
        self.dataset.save_observations(self.get_data('good_eats.csv'))

    def _save_observations_and_calculation(self, formula=None):
        self._save_observations()
        self.calculation = Calculation()
        return self._save_calculation(formula)

    def test_save(self):
        record = self._save_observations_and_calculation()
        self.assertTrue(isinstance(record, dict))
        self.assertTrue(Calculation.FORMULA in record.keys())
        self.assertTrue(Calculation.STATE in record.keys())
        record = Calculation.find(self.dataset)[0].record
        self.assertEqual(record[Calculation.STATE], Calculation.STATE_READY)
        self.assertTrue(Calculation(record).is_ready)

    def test_save_set_status(self):
        record = self._save_observations_and_calculation()
        self.assertTrue(isinstance(record, dict))
        self.assertTrue(Calculation.FORMULA in record.keys())

    def test_save_improper_formula(self):
        assert_raises(ParseError, self._save_observations_and_calculation,
                      'NON_EXISTENT_COLUMN')
        try:
            self._save_observations_and_calculation('NON_EXISTENT_COLUMN')
        except ParseError as e:
            self.assertTrue('Missing column' in e.__str__())

    def test_save_unparsable_formula(self):
        assert_raises(ParseError, self._save_observations_and_calculation,
                      '=NON_EXISTENT_COLUMN')
        try:
            self._save_observations_and_calculation(
                '=NON_EXISTENT_COLUMN')
        except ParseError as e:
            self.assertTrue('Parse Failure' in e.__str__())

    def test_save_improper_formula_no_data(self):
        assert_raises(ParseError, Calculation().save, self.dataset,
                      'NON_EXISTENT_COLUMN', self.name)
        try:
            Calculation().save(self.dataset, 'NON_EXISTENT_COLUMN',
                               self.name)
        except ParseError as e:
            self.assertTrue('No schema' in e.__str__())

    def test_save_unparsable_formula_no_data(self):
        assert_raises(ParseError, Calculation().save, self.dataset,
                      '=NON_EXISTENT_COLUMN', self.name)
        try:
            Calculation().save(self.dataset, '=NON_EXISTENT_COLUMN',
                               self.name)
        except ParseError as e:
            self.assertTrue('Parse Failure' in e.__str__())

    def test_save_non_existent_group(self):
        self._save_observations()
        assert_raises(ParseError, Calculation().save, self.dataset,
                      self.formula, self.name, group='NON_EXISTENT_GROUP')
        try:
            Calculation().save(self.dataset, self.formula, self.name,
                               group='NON_EXISTENT_GROUP')
        except ParseError as e:
            self.assertTrue('Group' in e.__str__())

    def test_find(self):
        record = self._save_observations_and_calculation()
        rows = Calculation.find(self.dataset)
        new_record = rows[0].record
        status = new_record.pop(Calculation.STATE)
        self.assertEqual(status, Calculation.STATE_READY)

    def test_sets_dependent_calculations(self):
        record = self._save_observations_and_calculation()
        self.name = 'test1'
        record = self._save_calculation('test')
        calculation = Calculation.find_one(self.dataset.dataset_id, 'test')
        self.assertEqual(calculation.dependent_calculations, ['test1'])

    def test_removes_dependent_calculations(self):
        record = self._save_observations_and_calculation()
        self.name = 'test1'
        record = self._save_calculation('test')
        calculation = Calculation.find_one(self.dataset.dataset_id, 'test')
        self.assertEqual(calculation.dependent_calculations, ['test1'])
        calculation = Calculation.find_one(self.dataset.dataset_id, 'test1')
        calculation.delete(self.dataset)
        calculation = Calculation.find_one(self.dataset.dataset_id, 'test')
        self.assertEqual(calculation.dependent_calculations, [])

    def test_disallow_delete_dependent_calculation(self):
        record = self._save_observations_and_calculation()
        self.name = 'test1'
        record = self._save_calculation('test')
        calculation = Calculation.find_one(self.dataset.dataset_id, 'test')
        self.assertEqual(calculation.dependent_calculations, ['test1'])
        calculation = Calculation.find_one(self.dataset.dataset_id, 'test')
        assert_raises(DependencyError, calculation.delete, self.dataset)
