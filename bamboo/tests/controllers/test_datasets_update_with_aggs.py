import simplejson as json

from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets_update import\
    TestAbstractDatasetsUpdate


class TestDatasetsUpdateWithAggs(TestAbstractDatasetsUpdate):

    def setUp(self):
        TestAbstractDatasetsUpdate.setUp(self)
        self._create_original_datasets()
        self._add_common_calculations()

        # create linked datasets
        aggregations = {
            'max(amount)': 'max of amount',
            'mean(amount)': 'mean of amount',
            'median(amount)': 'median of amount',
            'min(amount)': 'min of amount',
            'ratio(amount, gps_latitude)': 'ratio of amount and gps_latitude',
            'sum(amount)': 'sum of amount',
        }

        for aggregation, name in aggregations.items():
            self.calculations.create(
                self.dataset2_id, aggregation, name)

        # and with group
        for aggregation, name in aggregations.items():
            self.calculations.create(
                self.dataset2_id, aggregation, name, group='food_type')

        result = json.loads(
            self.controller.aggregations(self.dataset2_id))

        self.linked_dataset1_id = result['']

        # create merged datasets
        result = json.loads(self.controller.merge(
            datasets=json.dumps([self.dataset1_id, self.dataset2_id])))
        self.merged_dataset1_id = result[Dataset.ID]

        result = json.loads(self.controller.merge(
            datasets=json.dumps(
                [self.merged_dataset1_id, self.linked_dataset1_id])))
        self.merged_dataset2_id = result[Dataset.ID]

    def test_setup_datasets(self):
        self._verify_dataset(
            self.dataset1_id,
            'updates_with_aggs/originals/dataset1.p')
        self._verify_dataset(
            self.dataset2_id,
            'updates_with_aggs/originals/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'updates_with_aggs/originals/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'updates_with_aggs/originals/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates_with_aggs/originals/merged_dataset2.p')

    def test_datasets_update(self):
        self._put_row_updates(self.dataset2_id)
        self._verify_dataset(
            self.dataset2_id,
            'updates_with_aggs/update/dataset2.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'updates_with_aggs/update/merged_dataset1.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'updates_with_aggs/update/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates_with_aggs/update/merged_dataset2.p')
