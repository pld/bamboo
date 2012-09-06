from collections import defaultdict

import numpy as np

from lib.constants import SCHEMA
from models.dataset import Dataset
from models.observation import Observation
from test_calculator import TestCalculator


class TestAggregations(TestCalculator):

    AGGREGATION_RESULTS = {
        'sum(amount)': 2007.5,
        'sum(gps_latitude)': 624.089497667,
    }

    GROUP_AGGREGATION_RESULTS = {
        ('sum(amount)', 'caffeination'): 2.5,
        ('sum(amount)', 'deserts'): 8.75,
        ('sum(amount)', 'dinner'): 1659,
        ('sum(amount)', 'drunk_food'): 20,
        ('sum(amount)', 'lunch'): 271.25,
        ('sum(amount)', 'morning_food'): 40,
        ('sum(amount)', 'streat_sweets'): 4,
        ('sum(amount)', 'street_meat'): 2,
        ('sum(gps_latitude)', 'caffeination'): 37.9504400259,
        ('sum(gps_latitude)', 'deserts'): 82.0306681423,
        ('sum(gps_latitude)', 'dinner'): 78.1797630962,
        ('sum(gps_latitude)', 'drunk_food'): np.nan,
        ('sum(gps_latitude)', 'lunch'): 240.995748612,
        ('sum(gps_latitude)', 'morning_food'): 61.8834058679,
        ('sum(gps_latitude)', 'streat_sweets'): 82.0349438348,
        ('sum(gps_latitude)', 'street_meat'): 41.0145280883,
    }

    def setUp(self):
        TestCalculator.setUp(self)
        self.calculations = [
            'sum(amount)',
            'sum(gps_latitude)',
        ]
        self.expected_length = defaultdict(int)

    def _calculations_to_results(self, formula, row):
        if self.group:
            return self.GROUP_AGGREGATION_RESULTS[(formula, row[self.group])]
        else:
            return self.AGGREGATION_RESULTS[formula]

    def _test_calculation_results(self, name, formula):
        linked_dataset_id = self.dataset.linked_datasets[self.group or '']

        if self.group not in self.expected_length and self.group is not None:
            self.expected_length[self.group] = 1

        # add an extra column for the group names
        self.expected_length[self.group] += 1

        # retrieve linked dataset
        self.assertFalse(linked_dataset_id is None)
        linked_dataset = Dataset.find_one(linked_dataset_id)
        linked_dframe = Observation.find(linked_dataset, as_df=True)

        column_labels_to_slugs = linked_dataset.build_labels_to_slugs()
        name = column_labels_to_slugs[name]

        self.assertTrue(name in linked_dframe.columns)
        self.assertEqual(len(linked_dframe.columns),
                         self.expected_length[self.group])

        # test that the schema is up to date
        self.assertTrue(SCHEMA in linked_dataset.record.keys())
        self.assertTrue(isinstance(linked_dataset.data_schema, dict))
        schema = linked_dataset.data_schema

        # test slugified column names
        column_names = [name]
        if self.group:
            column_names.append(self.group)
        for column_name in column_names:
            self.assertTrue(column_name in schema.keys())

        for idx, row in linked_dframe.iterrows():
            result = np.float64(row[name])
            stored = self._calculations_to_results(formula, row)
            # np.nan != np.nan, continue if we have two nan values
            if np.isnan(result) and np.isnan(stored):
                continue
            msg = self._equal_msg(result, stored, formula)
            self.assertAlmostEqual(result, stored, self.places, msg)

    def test_calculator_with_delay(self):
        self._test_calculator()

    def test_calculator_without_delay(self):
        self._test_calculator(delay=False)

    def test_calculator_with_group(self):
        self.group = 'food_type'
        self._test_calculator(delay=False)
