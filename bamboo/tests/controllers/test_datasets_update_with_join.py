import json
import pickle

from bamboo.controllers.datasets import Datasets
from bamboo.models.dataset import Dataset
from bamboo.lib.datetools import recognize_dates
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasetsUpdateWithJoin(TestAbstractDatasets):

    def setUp(self):
        """
        These tests use the following dataset configuration:

            l -> left
            r -> right
            j -> joined

            l1   r2
             \  /
              j1

        Dependencies flow from top to bottom.
        """
        TestAbstractDatasets.setUp(self)
        self.controller = Datasets()

        # create original datasets
        self._post_file()
        self.left_dataset_id = self.dataset_id
        self._post_file('good_eats_aux.csv')
        self.right_dataset_id = self.dataset_id

        # create joined dataset
        self.on = 'food_type'
        results = json.loads(self.controller.join(
            self.left_dataset_id, self.right_dataset_id, on=self.on))
        self.joined_dataset_id = results[Dataset.ID]

    def _verify_dataset(self, dataset_id, fixture_path):
        dataset = Dataset.find_one(dataset_id)
        dframe = dataset.dframe()
        expected_dframe = recognize_dates(
            pickle.load(open(fixture_path, 'rb')))
        self._check_dframes_are_equal(dframe, expected_dframe)

    def test_setup_datasets(self):
        self._verify_dataset(
            self.left_dataset_id,
            'tests/fixtures/updates_with_join/originals/left_dataset.p')
        self._verify_dataset(
            self.right_dataset_id,
            'tests/fixtures/updates_with_join/originals/right_dataset.p')
        self._verify_dataset(
            self.joined_dataset_id,
            'tests/fixtures/updates_with_join/originals/joined_dataset.p')

    def test_datasets_update_left(self):
        self._put_row_updates(
            self.left_dataset_id,
            file_name='updates_with_join/update_left/update.json'
        )
        self._verify_dataset(
            self.left_dataset_id,
            'tests/fixtures/updates_with_join/update_left/left_dataset.p')
        self._verify_dataset(
            self.joined_dataset_id,
            'tests/fixtures/updates_with_join/update_left/joined_dataset.p')

    def test_datasets_update_left_no_join_col(self):
        self._put_row_updates(
            self.left_dataset_id,
            file_name='updates_with_join/update_left_no_join_co'
            'l/update.json')
        self._verify_dataset(
            self.left_dataset_id,
            'tests/fixtures/updates_with_join/update_left_no_join_col/left_dat'
            'aset.p')
        self._verify_dataset(
            self.joined_dataset_id,
            'tests/fixtures/updates_with_join/update_left_no_join_col/joined_d'
            'ataset.p')

    def test_datasets_update_right(self):
        self._put_row_updates(
            self.left_dataset_id,
            file_name='updates_with_join/update_left/update_baked_goods.json'
        )
        self._put_row_updates(
            self.right_dataset_id,
            file_name='updates_with_join/update_right/update.js'
            'on'
        )
        self._verify_dataset(
            self.left_dataset_id,
            'tests/fixtures/updates_with_join/update_right/left_dataset.p')
        self._verify_dataset(
            self.right_dataset_id,
            'tests/fixtures/updates_with_join/update_right/right_dataset.p')
        self._verify_dataset(
            self.joined_dataset_id,
            'tests/fixtures/updates_with_join/update_right/joined_dataset.p')

    def test_datasets_update_right_non_unique_join(self):
        self._put_row_updates(
            self.right_dataset_id,
            file_name='updates_with_join/update_right/update_non_unique.json',
            validate=False
        )
        self._verify_dataset(
            self.right_dataset_id,
            'tests/fixtures/updates_with_join/originals/right_dataset.p')
        self._verify_dataset(
            self.joined_dataset_id,
            'tests/fixtures/updates_with_join/originals/joined_dataset.p')
