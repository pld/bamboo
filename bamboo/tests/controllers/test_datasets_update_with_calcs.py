import simplejson as json

from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets_update import\
    TestAbstractDatasetsUpdate


class TestDatasetsUpdateWithCalcs(TestAbstractDatasetsUpdate):

    def setUp(self):
        TestAbstractDatasetsUpdate.setUp(self)
        self._create_original_datasets()
        self._add_common_calculations()

        # create linked datasets
        self.calculations.create(
            self.dataset2_id, 'sum(amount)', 'sum of amount')
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
            'updates_with_calcs/originals/dataset1.p')
        self._verify_dataset(
            self.dataset2_id,
            'updates_with_calcs/originals/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'updates_with_calcs/originals/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'updates_with_calcs/originals/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates_with_calcs/originals/merged_dataset2.p')

    def _add_calculations(self):
        self.calculations.create(self.dataset2_id,
                                 'amount_plus_gps_alt > gps_precision',
                                 'amount plus gps_alt > gps_precision')
        self.calculations.create(self.linked_dataset1_id,
                                 'sum_of_amount * 2',
                                 'amount')
        self.calculations.create(self.merged_dataset1_id,
                                 'gps_alt * 2',
                                 'double gps_alt')
        self.calculations.create(self.merged_dataset2_id,
                                 'amount * 2',
                                 'double amount')
        self._verify_dataset(
            self.dataset2_id,
            'updates_with_calcs/calcs/dataset2.p')
        self._verify_dataset(
            self.linked_dataset1_id,
            'updates_with_calcs/calcs/linked_dataset1.p')
        self._verify_dataset(
            self.merged_dataset1_id,
            'updates_with_calcs/calcs/merged_dataset1.p')
        self._verify_dataset(
            self.merged_dataset2_id,
            'updates_with_calcs/calcs/merged_dataset2.p')

    def test_datasets_add_calculations(self):
        self._add_calculations()
