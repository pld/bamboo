import numpy as np

from bamboo.lib.datetools import parse_date_to_unix_time
from bamboo.models.dataset import Dataset
from bamboo.tests.core.test_calculator import TestCalculator


CALCS_TO_DEPS = {
    # constants
    '-9 + 5': [],

    # aliases
    'rating': ['rating'],
    'gps': ['gps'],

    # arithmetic
    'amount + gps_alt': ['amount', 'gps_alt'],
    'amount - gps_alt': ['amount', 'gps_alt'],
    'amount + 5': ['amount'],
    'amount - gps_alt + 2.5': ['amount', 'gps_alt'],
    'amount * gps_alt': ['amount', 'gps_alt'],
    'amount / gps_alt': ['amount', 'gps_alt'],
    'amount * gps_alt / 2.5': ['amount', 'gps_alt'],
    'amount + gps_alt * gps_precision': ['amount', 'gps_alt',
                                         'gps_precision'],

    # precedence
    '(amount + gps_alt) * gps_precision': ['amount', 'gps_alt',
                                           'gps_precision'],

    # comparison
    'amount == 2': ['amount'],
    '10 < amount': ['amount'],
    '10 < amount + gps_alt': ['amount', 'gps_alt'],

    # logical
    'not amount == 2': ['amount'],
    'not(amount == 2)': ['amount'],
    'amount == 2 and 10 < amount': ['amount'],
    'amount == 2 or 10 < amount': ['amount'],
    'not not amount == 2 or 10 < amount': ['amount'],
    'not amount == 2 or 10 < amount': ['amount'],
    '(not amount == 2) or 10 < amount': ['amount'],
    'not(amount == 2 or 10 < amount)': ['amount'],
    'amount ^ 3': ['amount'],
    '(amount + gps_alt) ^ 2 + 100': ['amount', 'gps_alt'],
    '-amount': ['amount'],
    '-amount < gps_alt - 100': ['amount', 'gps_alt'],

    # membership
    'rating in ["delectible"]': ['rating'],
    'risk_factor in ["low_risk"]': ['risk_factor'],
    'amount in ["9.0", "2.0", "20.0"]': ['amount'],
    '(risk_factor in ["low_risk"]) and (amount in ["9.0", "20.0"])':
    ['risk_factor', 'amount'],

    # dates
    'date("09-04-2012") - submit_date > 21078000': ['submit_date'],
    'today() - submit_date': ['submit_date'],

    # cases
    'case food_type in ["morning_food"]: 1, food_type in ["lunch"]: 2,'
    ' default: 3': ['food_type'],
    'case food_type in ["morning_food"]: 1, food_type in ["lunch"]: 2':
    ['food_type'],

    # row-wise column-based aggregations
    'percentile(amount)': ['amount']
}

DYNAMIC = ['today() - submit_date']


class TestCalculations(TestCalculator):

    def setUp(self):
        TestCalculator.setUp(self)
        self.calculations = CALCS_TO_DEPS.keys()
        self.dynamic_calculations = DYNAMIC

    def _test_calculation_results(self, name, formula):
            unslug_name = name
            labels = self.column_labels_to_slugs.keys()
            self.assertTrue(name in labels, '%s not in %s' % (name, labels))

            name = self.column_labels_to_slugs[unslug_name]

            # test that updated dataframe persisted
            self.dframe = self.dataset.dframe()
            self.assertTrue(name in self.dframe.columns, '%s not in %s' %
                            (name, self.dframe.columns))

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
            self._test_cached_dframe(name, formula,
                                     formula in self.dynamic_calculations)

    def _test_cached_dframe(self, name, original_formula, dynamic):
        formula = not dynamic and self.column_labels_to_slugs[original_formula]

        for idx, row in self.dframe.iterrows():
            try:
                result = np.float64(row[name])
                places = self.places

                if dynamic:
                    stored = parse_date_to_unix_time(self.now) -\
                        parse_date_to_unix_time(row['submit_date'])
                    # large approximate window for time compares
                    places = 2
                else:
                    stored = np.float64(row[formula])

                # one np.nan != np.nan, continue if we have two nan values
                if np.isnan(result) and np.isnan(stored):
                    continue

                msg = self._equal_msg(result, stored, original_formula)
                self.assertAlmostEqual(result, stored, places, msg)
            except ValueError:
                msg = self._equal_msg(row[name], row[formula], formula)
                self.assertEqual(row[name], row[formula], msg)

    def test_calculator(self):
        self._test_calculator()
