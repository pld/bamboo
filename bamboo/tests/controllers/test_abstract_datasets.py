import json

import cherrypy

from controllers.datasets import Datasets
from lib.constants import ID, SCHEMA
from lib.io import create_dataset_from_url
from lib.utils import df_to_jsondict, series_to_jsondict
from tests.test_base import TestBase


class TestAbstractDatasets(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Datasets()
        self._file_name = 'good_eats.csv'
        self._update_file_name = 'good_eats_update.json'
        self._update_file_path = 'tests/fixtures/%s' % self._update_file_name
        self._update_check_file_name = 'good_eats_update_values.json'
        self._update_check_file_path = 'tests/fixtures/%s' %\
            self._update_check_file_name

    def _post_row_updates(self, dataset_id=None):
        if not dataset_id:
            dataset_id = self.dataset_id
        # mock the cherrypy server by setting the POST request body
        cherrypy.request.body = open(self._update_file_path, 'r')
        result = json.loads(self.controller.PUT(dataset_id=dataset_id))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(ID in result)
        # set up the values to test against
        with open(self._update_check_file_path, 'r') as f:
            self._update_values = json.loads(f.read())

    def _post_file(self, file_name=None):
        if file_name is None:
            file_name = self._file_name
        self.dataset_id = create_dataset_from_url(
            'file://tests/fixtures/%s' % file_name, allow_local_file=True)[ID]
        self.schema = json.loads(self.controller.GET(self.dataset_id,
                                 mode=self.controller.MODE_INFO))[SCHEMA]

    def _check_dframes_are_equal(self, dframe1, dframe2):
        self._check_dframe_is_subset(dframe1, dframe2)
        self._check_dframe_is_subset(dframe2, dframe1)

    def _check_dframe_is_subset(self, dframe1, dframe2):
        dframe2_rows = df_to_jsondict(dframe2)
        for row in dframe1.iterrows():
            dframe1_row = series_to_jsondict(row[1])
            self.assertTrue(dframe1_row in dframe2_rows,
                            'dframe1_row: %s\n\ndframe2_rows: %s' % (
                                dframe1_row, dframe2_rows))
