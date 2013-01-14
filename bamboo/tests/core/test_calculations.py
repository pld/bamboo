import numpy as np

from bamboo.models.dataset import Dataset
from bamboo.tests.core.test_calculator import TestCalculator


class TestCalculations(TestCalculator):

    def setUp(self):
        TestCalculator.setUp(self)
        self.calculations = [
            # constants
            '-9 + 5',

            # aliases
            'rating',
            'gps',

            # arithmetic
            'amount + gps_alt',
            'amount - gps_alt',
            'amount + 5',
            'amount - gps_alt + 2.5',
            'amount * gps_alt',
            'amount / gps_alt',
            'amount * gps_alt / 2.5',
            'amount + gps_alt * gps_precision',

            # precedence
            '(amount + gps_alt) * gps_precision',

            # comparison
            'amount == 2',
            '10 < amount',
            '10 < amount + gps_alt',

            # logical
            'not amount == 2',
            'not(amount == 2)',
            'amount == 2 and 10 < amount',
            'amount == 2 or 10 < amount',
            'not not amount == 2 or 10 < amount',
            'not amount == 2 or 10 < amount',
            '(not amount == 2) or 10 < amount',
            'not(amount == 2 or 10 < amount)',
            'amount ^ 3',
            '(amount + gps_alt) ^ 2 + 100',
            '-amount',
            '-amount < gps_alt - 100',

            # membership
            'rating in ["delectible"]',
            'risk_factor in ["low_risk"]',
            'amount in ["9.0", "2.0", "20.0"]',
            '(risk_factor in ["low_risk"]) and (amount in ["9.0", "20.0"])',

            # dates
            'date("09-04-2012") - submit_date > 21078000',

            # cases
            'case food_type in ["morning_food"]: 1, food_type in ["lunch"]: 2,'
            ' default: 3',
            'case food_type in ["morning_food"]: 1, food_type in ["lunch"]: 2',

            # row-wise column-based aggregations
            'percentile(amount)',
        ]

    def _test_calculation_results(self, name, formula):
            unslug_name = name
            name = self.column_labels_to_slugs[unslug_name]

            # test that updated dataframe persisted
            self.dframe = self.dataset.dframe()
            self.assertTrue(name in self.dframe.columns)

            # test new number of columns
            self.added_num_cols += 1
            self.assertEqual(self.start_num_cols + self.added_num_cols,
                             len(self.dframe.columns.tolist()))

            # test that the schema is up to date
            dataset = Dataset.find_one(self.dataset.dataset_id)
            self.assertTrue(Dataset.SCHEMA in dataset.record.keys())
            self.assertTrue(isinstance(dataset.schema, dict))
            schema = dataset.schema

            # test slugified column names
            self.slugified_key_list.append(name)
            self.assertEqual(sorted(schema.keys()),
                             sorted(self.slugified_key_list))

            # test column labels
            self.label_list.append(unslug_name)
            labels = [schema[col][Dataset.LABEL] for col in schema.keys()]
            self.assertEqual(sorted(labels), sorted(self.label_list))

            # test result of calculation
            formula = self.column_labels_to_slugs[formula]

            for idx, row in self.dframe.iterrows():
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

    def test_calculator(self):
        self._test_calculator()
