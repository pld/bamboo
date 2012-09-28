from collections import defaultdict
import pickle

import numpy as np

from lib.constants import SCHEMA
from lib.utils import GROUP_DELIMITER
from models.dataset import Dataset
from models.observation import Observation
from test_calculator import TestCalculator


class TestAggregations(TestCalculator):

    AGGREGATION_RESULTS = {
        'max(amount)': 1600,
        'mean(amount)': 105.65789473684211,
        'median(amount)': 12,
        'min(amount)': 2.0,
        'sum(amount)': 2007.5,
        'sum(gps_latitude)': 624.089497667,
        'ratio(amount, gps_latitude)': 3.184639,
        'sum(risk_factor in ["low_risk"])': 18,
        'ratio(risk_factor in ["low_risk"], risk_factor in ["low_risk",'
        ' "medium_risk"])': 18.0 / 19,
        'ratio(risk_factor in ["low_risk"], 1)': 18.0 / 19,
    }

    GROUP_TO_RESULTS = {
        'food_type':
        pickle.load(
            open('tests/fixtures/good_eats_agg_group_food_type.p', 'rb')),
        'food_type,rating':
        pickle.load(
            open('tests/fixtures/good_eats_agg_group_food_type_rating.p',
                 'rb')),
    }

    def setUp(self):
        TestCalculator.setUp(self)
        self.calculations = [
            'max(amount)',
            'mean(amount)',
            'median(amount)',
            'min(amount)',
            'sum(amount)',
            'sum(gps_latitude)',
            'ratio(amount, gps_latitude)',
            'sum(risk_factor in ["low_risk"])',
            'ratio(risk_factor in ["low_risk"], risk_factor in ["low_risk",'
            ' "medium_risk"])',
            'ratio(risk_factor in ["low_risk"], 1)',
        ]
        self.expected_length = defaultdict(int)
        self.groups_list = None

    def _offset_for_ratio(self, formula, _int):
        if formula[0:5] == 'ratio':
            _int += 2
        return _int

    def _get_initial_len(self, formula, groups_list):
        initial_len = 0 if self.group == '' else len(groups_list)
        return self._offset_for_ratio(formula, initial_len)

    def _columns_per_aggregation(self, formula):
        initial_len = 1
        return self._offset_for_ratio(formula, initial_len)

    def _calculations_to_results(self, formula, row):
        if self.group:
            res = self.GROUP_TO_RESULTS[self.group][formula]
            column = row[self.groups_list[0]] if len(self.groups_list) <= 1\
                else tuple([row[group] for group in self.groups_list])
            res = res[column]
            return res
        else:
            return self.AGGREGATION_RESULTS[formula]

    def _test_calculation_results(self, name, formula):
        if self.group:
            self.groups_list = self.group.split(GROUP_DELIMITER)
        else:
            self.group = ''

        if not self.group in self.expected_length:
            self.expected_length[self.group] = self._get_initial_len(
                formula, self.groups_list)

        # add an extra column for the group names
        self.expected_length[self.group] += self._columns_per_aggregation(
            formula)

        # retrieve linked dataset
        linked_dataset = self.dataset.linked_datasets[self.group]
        self.assertFalse(linked_dataset is None)
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
        if self.groups_list:
            column_names.extend(self.groups_list)
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

    def test_calculator_with_group_list(self):
        self.group = 'food_type'
        self._test_calculator(delay=False)

    def test_calculator_with_multigroup_list(self):
        self.group = 'food_type,rating'
        self._test_calculator(delay=False)
