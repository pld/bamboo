import numpy as np

from tests.test_base import TestBase

from lib.constants import DATASET_ID, SCHEMA, SIMPLETYPE
from lib.mongo import _encode_for_mongo
from lib.tasks.calculator import calculate_column
from models.dataset import Dataset
from models.observation import Observation


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.test_dataset_ids['good_eats.csv'])
        Observation.save(self.test_data['good_eats.csv'], self.dataset)
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
            'rating in ["delectible"]',
            'risk_factor in ["low_risk"]',
            'amount in ["9.0", "2.0", "20.0"]',
            '(risk_factor in ["low_risk"]) and (amount in ["9.0", "20.0"])',
        ]
        self.places = 5

    def _equal_msg(self, calculated, stored, formula):
        return '(calculated %s) %s != (stored %s) %s (within %s places), formula: %s'\
                % (type(calculated), calculated, type(stored), stored,
                        self.places, formula)

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

            # test that the schema is up to date
            dataset = Dataset.find_one(self.dataset[DATASET_ID])
            self.assertTrue(SCHEMA in dataset.keys())
            self.assertTrue(isinstance(dataset[SCHEMA], dict))
            schema = dataset[SCHEMA]
            encoded_formula = _encode_for_mongo(formula)
            self.assertTrue(encoded_formula in schema.keys(),
                    '%s: in %s' % (encoded_formula, schema.keys()))

            # test result of calculation
            for idx, row in dframe.iterrows():
                formula = _encode_for_mongo(formula)
                try:
                    result = np.float64(row[name])
                    stored = np.float64(row[formula])
                    # np.nan != np.nan, continue if we have two nan values
                    if np.isnan(result) and np.isnan(stored):
                        continue
                    msg = self._equal_msg(result, stored, formula)
                    self.assertAlmostEqual(result, stored, self.places, msg)
                except ValueError:
                    msg = self._equal_msg(row[name], row[formula], formula)
                    self.assertEqual(row[name], row[formula], msg)

    def test_calculator_with_delay(self):
        self._test_calculator()

    def test_calculator_without_delay(self):
        self._test_calculator(delay=False)
