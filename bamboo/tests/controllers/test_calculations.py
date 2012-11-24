import json
from time import sleep

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.core.frame import DATASET_ID
from bamboo.lib.io import create_dataset_from_url
from bamboo.models.calculation import Calculation
from bamboo.models.dataset import Dataset
from bamboo.tests.decorators import requires_async
from bamboo.tests.test_base import TestBase


class TestCalculations(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset_id = create_dataset_from_url(
            '%s%s' % (self._local_fixture_prefix(), 'good_eats.csv'),
            allow_local_file=True).dataset_id
        self.controller = Calculations()
        self.formula = 'amount + gps_alt'
        self.name = 'test'

    def _post_formula(self):
        return self.controller.create(self.dataset_id, self.formula, self.name)

    def test_show(self):
        self._post_formula()
        response = self.controller.show(self.dataset_id)
        self.assertTrue(isinstance(json.loads(response), list))

    def test_create(self):
        response = json.loads(self._post_formula())
        self.assertTrue(isinstance(response, dict))
        self.assertTrue(self.controller.SUCCESS in response)
        self.assertTrue(self.dataset_id in response[self.controller.SUCCESS])

    @requires_async
    def test_create_async_not_ready(self):
        self.dataset_id = create_dataset_from_url(
            '%s%s' % (self._local_fixture_prefix(), 'good_eats_huge.csv'),
            allow_local_file=True).dataset_id
        response = json.loads(self._post_formula())
        dataset = Dataset.find_one(self.dataset_id)
        self.assertFalse(dataset.is_ready)
        self.assertTrue(isinstance(response, dict))
        self.assertFalse(DATASET_ID in response)
        while True:
            dataset = Dataset.find_one(self.dataset_id)
            if dataset.is_ready:
                break
            sleep(self.SLEEP_DELAY)
        self.assertFalse(self.name in dataset.schema.keys())

    @requires_async
    def test_create_async_sets_calculation_status(self):
        self.dataset_id = create_dataset_from_url(
            '%s%s' % (self._local_fixture_prefix(), 'good_eats_huge.csv'),
            allow_local_file=True).dataset_id
        while True:
            dataset = Dataset.find_one(self.dataset_id)
            if dataset.is_ready:
                break
            sleep(self.SLEEP_DELAY)
        response = json.loads(self._post_formula())
        self.assertTrue(isinstance(response, dict))
        self.assertTrue(self.controller.SUCCESS in response)
        self.assertTrue(self.dataset_id in response[self.controller.SUCCESS])
        response = json.loads(self.controller.show(self.dataset_id))[0]
        self.assertTrue(isinstance(response, dict))
        self.assertTrue(Calculation.STATE in response)
        self.assertEqual(response[Calculation.STATE],
                         Calculation.STATE_PENDING)
        while True:
            response = json.loads(self.controller.show(self.dataset_id))[0]
            dataset = Dataset.find_one(self.dataset_id)
            if response[Calculation.STATE] != Calculation.STATE_PENDING:
                break
            sleep(self.SLEEP_DELAY)
        self.assertEqual(response[Calculation.STATE],
                         Calculation.STATE_READY)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(self.name in dataset.schema.keys())

    @requires_async
    def test_create_async(self):
        while True:
            dataset = Dataset.find_one(self.dataset_id)
            if dataset.is_ready:
                break
            sleep(self.SLEEP_DELAY)
        response = json.loads(self._post_formula())
        self.assertTrue(isinstance(response, dict))
        self.assertTrue(self.controller.SUCCESS in response)
        self.assertTrue(self.dataset_id in response[self.controller.SUCCESS])
        while True:
            response = json.loads(self.controller.show(self.dataset_id))[0]
            dataset = Dataset.find_one(self.dataset_id)
            if response[Calculation.STATE] != Calculation.STATE_PENDING:
                break
            sleep(self.SLEEP_DELAY)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(self.name in dataset.schema.keys())

    def test_create_invalid_formula(self):
        result = json.loads(
            self.controller.create(self.dataset_id, '=NON_EXIST', self.name))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Datasets.ERROR in result.keys())

    def test_create_remove_summary(self):
        Datasets().summary(
            self.dataset_id,
            select=Datasets.SELECT_ALL_FOR_SUMMARY)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(isinstance(dataset.stats, dict))
        self.assertTrue(isinstance(dataset.stats[Dataset.ALL], dict))
        self._post_formula()
        # stats should have new column for calculation
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(self.name in dataset.stats.get(Dataset.ALL).keys())

    def test_delete_nonexistent_calculation(self):
        result = json.loads(self.controller.delete(self.dataset_id, self.name))
        self.assertTrue(Calculations.ERROR in result)

    def test_delete(self):
        self._post_formula()
        result = json.loads(self.controller.delete(self.dataset_id, self.name))
        self.assertTrue(AbstractController.SUCCESS in result)
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue(self.name not in dataset.build_labels_to_slugs())

    def test_show_jsonp(self):
        self._post_formula()
        results = self.controller.show(self.dataset_id, callback='jsonp')
        self.assertEqual('jsonp(', results[0:6])
        self.assertEqual(')', results[-1])

    def test_create_aggregation(self):
        self.formula = 'sum(amount)'
        self.name = 'test'
        response = json.loads(self._post_formula())
        self.assertTrue(isinstance(response, dict))
        self.assertTrue(self.controller.SUCCESS in response)
        self.assertTrue(self.dataset_id in response[self.controller.SUCCESS])
        dataset = Dataset.find_one(self.dataset_id)
        self.assertTrue('' in dataset.aggregated_datasets_dict.keys())

    def test_delete_aggregation(self):
        self.formula = 'sum(amount)'
        self.name = 'test'
        response = json.loads(self._post_formula())
        result = json.loads(
            self.controller.delete(self.dataset_id, self.name, ''))
        self.assertTrue(AbstractController.SUCCESS in result)
        dataset = Dataset.find_one(self.dataset_id)
        agg_dataset = Dataset.find_one(dataset.aggregated_datasets_dict[''])
        self.assertTrue(self.name not in agg_dataset.build_labels_to_slugs())

    def test_error_on_delete_calculation_with_dependency(self):
        self._post_formula()
        dep_name = self.name
        self.formula = dep_name
        self.name = 'test1'
        response = json.loads(self._post_formula())
        self.assertTrue(isinstance(response, dict))
        self.assertTrue(self.controller.SUCCESS in response)
        result = json.loads(
            self.controller.delete(self.dataset_id, dep_name, ''))
        self.assertTrue(AbstractController.ERROR in result)
        self.assertTrue('depend' in result[AbstractController.ERROR])
