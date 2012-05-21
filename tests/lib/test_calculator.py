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
        ]

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
                    self.assertAlmostEqual(float(row[name]),
                            float(row[formula]))
                except ValueError:
                    self.assertEqual(row[name], row[formula])

    def test_calculator_with_delay(self):
        self._test_calculator()

    def test_calculator_without_delay(self):
        self._test_calculator(delay=False)
