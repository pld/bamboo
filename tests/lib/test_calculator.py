from numpy import float64

from tests.test_base import TestBase

from lib.tasks.calculator import calculate_column
from models.dataset import Dataset
from models.observation import Observation
from lib.mongo import _encode_for_mongo


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.dataset_id)
        Observation.save(self.data, self.dataset)
        self.calculations = [
            'rating',
            'gps',
            'amount + gps_alt',
            'amount - gps_alt',
            'amount + 5',
            'amount - gps_alt + 2.5',
            'amount * gps_alt',
            'amount / gps_alt',
            'amount * gps_alt / 2.5',
            'amount + gps_alt * gps_precision',
            '(amount + gps_alt) * gps_precision',
            'amount = 2',
            '10 < amount',
            '10 < amount + gps_alt',
            'not amount = 2',
            'not(amount = 2)',
            'amount = 2 and 10 < amount',
            'amount = 2 or 10 < amount',
            'not not amount = 2 or 10 < amount',
            'not amount = 2 or 10 < amount',
            '(not amount = 2) or 10 < amount',
            'not(amount = 2 or 10 < amount)',
            'amount ^ 3',
            '(amount + gps_alt) ^ 2 + 100',
            '-amount',
            '-amount < gps_alt - 100',
        ]
        self.places = 5

    def _equal_msg(self, calculated, stored, formula):
        return '(calculated) %s != (stored) %s (within %s places), formula: %s'\
                % (calculated, stored, self.places, formula)

    def _test_calculator(self, delay=True):
        dframe = Observation.find(self.dataset, as_df=True)

        for idx, formula in enumerate(self.calculations):
            name = 'test-%s' % idx
            if delay:
                task = calculate_column.delay(self.dataset, dframe,
                        formula, name)
                # test that task has completed
                self.assertTrue(task.ready())
                self.assertTrue(task.successful())
            else:
                task = calculate_column(self.dataset, dframe,
                        formula, name)

            # test that updated dataframe persisted
            dframe = Observation.find(self.dataset, as_df=True)
            self.assertTrue(name in dframe.columns)

            # test result of calculation
            for idx, row in dframe.iterrows():
                formula = _encode_for_mongo(formula)
                try:
                    msg = self._equal_msg(float64(row[name]),
                            float64(row[formula]), formula)
                    self.assertAlmostEqual(float64(row[name]),
                            float64(row[formula]), self.places, msg)
                except ValueError:
                    msg = self._equal_msg(row[name], row[formula], formula)
                    self.assertEqual(row[name], row[formula], msg)

    def test_calculator_with_delay(self):
        self._test_calculator()

    def test_calculator_without_delay(self):
        self._test_calculator(delay=False)
