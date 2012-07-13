import numpy as np

from lib.constants import LINKED_DATASETS, SCHEMA
from lib.utils import build_labels_to_slugs, slugify_columns
from models.dataset import Dataset
from models.observation import Observation
from test_calculator import TestCalculator


class TestAggregations(TestCalculator):

    AGGREGATION_RESULTS = {
        'sum(amount)': 2007.5,
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
    }

    def setUp(self):
        TestCalculator.setUp(self)
        self.calculations = [
            'sum(amount)',
        ]

    def _calculations_to_results(self, formula, row):
        if self.group:
            return self.GROUP_AGGREGATION_RESULTS[(formula, row[self.group])]
        else:
            return self.AGGREGATION_RESULTS[formula]

    def _test_calculation_results(self, name, formula):
        linked_dataset_ids = self.dataset[LINKED_DATASETS]
        self.assertEqual(len(linked_dataset_ids), 1)

        for linked_dataset_id in linked_dataset_ids:
            linked_dataset = Dataset.find_one(linked_dataset_id)
            linked_dframe = Observation.find(linked_dataset, as_df=True)

            column_labels_to_slugs = build_labels_to_slugs(linked_dataset)
            name = column_labels_to_slugs[name]

            self.assertTrue(name in linked_dframe.columns)

            # test that the schema is up to date
            self.assertTrue(SCHEMA in linked_dataset.keys())
            self.assertTrue(isinstance(linked_dataset[SCHEMA], dict))
            schema = linked_dataset[SCHEMA]

            # test slugified column names
            column_names = [name]
            if self.group:
                column_names.append(self.group)
                column_names = sorted(column_names)
            self.assertEqual(sorted(schema.keys()),
                             column_names)

            for idx, row in linked_dframe.iterrows():
                result = np.float64(row[name])
                stored = self._calculations_to_results(formula, row)
                msg = self._equal_msg(result, stored, formula)
                self.assertAlmostEqual(result, stored, self.places, msg)

    def test_calculator_with_delay(self):
        self._test_calculator()

    def test_calculator_without_delay(self):
        self._test_calculator(delay=False)

    def test_calculator_with_group(self):
        self.group = 'food_type'
        self._test_calculator(delay=False)
