import json
import os

import cherrypy

from bamboo.controllers.datasets import Datasets
from bamboo.core.frame import BambooFrame
from bamboo.lib.io import create_dataset_from_url
from bamboo.lib.jsontools import series_to_jsondict
from bamboo.models.dataset import Dataset
from bamboo.tests.test_base import TestBase


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

    def _put_row_updates(self, dataset_id=None, file_path=None, validate=True):
        if not dataset_id:
            dataset_id = self.dataset_id
        # mock the cherrypy server by setting the POST request body
        if not file_path:
            file_path = self._update_file_path
        cherrypy.request.body = open(file_path, 'r')
        result = json.loads(self.controller.update(dataset_id=dataset_id))
        if validate:
            self.assertTrue(isinstance(result, dict))
            self.assertTrue(Dataset.ID in result)
        # set up the (default) values to test against
        with open(self._update_check_file_path, 'r') as f:
            self._update_values = json.loads(f.read())

    def _post_file(self, file_name=None):
        if file_name is None:
            file_name = self._file_name
        self.dataset_id = create_dataset_from_url(
            'file://localhost%s/tests/fixtures/%s' % (os.getcwd(), file_name),
            allow_local_file=True).dataset_id
        self.schema = json.loads(
            self.controller.info(self.dataset_id))[Dataset.SCHEMA]

    def _check_dframes_are_equal(self, dframe1, dframe2):
        self._check_dframe_is_subset(dframe1, dframe2)
        self._check_dframe_is_subset(dframe2, dframe1)

    def _check_dframe_is_subset(self, dframe1, dframe2):
        dframe2_rows = BambooFrame(dframe2).to_jsondict()
        for row in dframe1.iterrows():
            dframe1_row = series_to_jsondict(row[1])
            self.assertTrue(dframe1_row in dframe2_rows,
                            'dframe1_row: %s\n\ndframe2_rows: %s' % (
                                dframe1_row, dframe2_rows[-1]))
