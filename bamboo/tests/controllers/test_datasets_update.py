import simplejson as json

from bamboo.controllers.calculations import Calculations
from bamboo.core.frame import PARENT_DATASET_ID
from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets_update import\
    TestAbstractDatasetsUpdate


class TestDatasetsUpdate(TestAbstractDatasetsUpdate):

    def setUp(self):
        TestAbstractDatasetsUpdate.setUp(self)
        self._create_original_datasets()

        # create linked datasets
        self.calculations = Calculations()
        self.name1 = 'sum of amount'
        self.formula1 = 'sum(amount)'
        self.calculations.create(self.dataset2_id, self.formula1, self.name1)
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
        self._verify_dataset(self.dataset1_id,
                             'updates/originals/dataset1.p')
        self._verify_dataset(self.dataset2_id,
                             'updates/originals/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'updates/originals/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'updates/originals/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates/originals/merged_dataset2.p')

    def _test_update1(self):
        for dataset_id in [self.merged_dataset1_id, self.merged_dataset2_id]:
            merged_dataset = Dataset.find_one(dataset_id)
            merged_rows = merged_dataset.observations()
            for row in merged_rows:
                self.assertTrue(PARENT_DATASET_ID in row.keys())

        self._verify_dataset(self.dataset1_id,
                             'updates/update1/dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'updates/update1/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates/update1/merged_dataset2.p')

    def test_datasets_update1(self):
        self._put_row_updates(self.dataset1_id)
        self._test_update1()

    def test_datasets_update1_and_update2(self):
        self._put_row_updates(self.dataset1_id)
        self._test_update1()
        self._put_row_updates(self.dataset2_id)
        self._verify_dataset(
            self.merged_dataset1_id,
            'updates/update2/merged_dataset1.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'updates/update2/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates/update2/merged_dataset2.p')

    def test_datasets_update_merged(self):
        self._put_row_updates(self.merged_dataset1_id)
        self._verify_dataset(
            self.merged_dataset1_id,
            'updates/update_merged/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates/update_merged/merged_dataset2.p')

    def test_datasets_update_aggregated_dataset(self):
        self._put_row_updates(
            dataset_id=self.linked_dataset1_id,
            file_name='updates/update_agg/update.json')
        self._verify_dataset(
            self.linked_dataset1_id,
            'updates/update_agg/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates/update_agg/merged_dataset2.p')
        self._put_row_updates(self.dataset2_id)
        self._verify_dataset(
            self.dataset2_id,
            'updates/update_agg2/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'updates/update_agg2/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates/update_agg2/merged_dataset2.p')
