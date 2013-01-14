import json
import os
from tempfile import NamedTemporaryFile

from pandas import concat

from bamboo.controllers.datasets import Datasets
from bamboo.models.dataset import Dataset
from bamboo.tests.decorators import run_profiler
from bamboo.tests.mock import MockUploadedFile
from bamboo.tests.test_base import TestBase


class TestProfile(TestBase):

    TEST_CASE_SIZES = {
        'tiny': (1, 1),
        'small': (2, 2),
        'large': (4, 40),
    }

    def setUp(self):
        TestBase.setUp(self)
        self.datasets = Datasets()
        self.tmp_file = NamedTemporaryFile(delete=False)

    def tearDown(self):
        os.unlink(self.tmp_file.name)

    def _expand_width(self, df, exponent):
        for i in xrange(0, exponent):
            other = df.rename(
                columns={col: '%s-%s' % (col, idx) for (idx, col) in
                         enumerate(df.columns)})
            df = df.join(other)
            df.rename(columns={col: str(idx) for (idx, col) in
                      enumerate(df.columns)}, inplace=True)
        return df

    def _grow_test_data(self, dataset_name, width_exp, length_factor):
        df = self.get_data(dataset_name)
        df = self._expand_width(df, width_exp)
        return concat([df] * length_factor)

    def test_tiny_profile(self):
        self._test_profile('tiny')

    def test_small_profile(self):
        self._test_profile('small')

    def test_large_profile(self):
        self._test_profile('large')

    @run_profiler
    def _test_profile(self, size):
        print 'bamboo/bamboo: %s' % size
        self._test_create_data(*self.TEST_CASE_SIZES[size])
        print 'saving dataset'
        self._test_save_dataset()
        self._test_get_info()
        self._test_get_summary()
        self._test_get_summary_with_group('province')
        self._test_get_summary_with_group('school_zone')

    def _test_create_data(self, width_exp, length_factor):
        self.data = self._grow_test_data(
            'kenya_secondary_schools_2007.csv', width_exp, length_factor)
        print 'bamboo/bamboo rows: %s, columns: %s' % (
            len(self.data), len(self.data.columns))

    def _test_save_dataset(self):
        self.data.to_csv(self.tmp_file)
        self.tmp_file.close()
        mock_uploaded_file = MockUploadedFile(open(self.tmp_file.name, 'r'))
        result = json.loads(self.datasets.create(csv_file=mock_uploaded_file))
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Dataset.ID in result)
        self.dataset_id = result[Dataset.ID]

    def _test_get_info(self):
        result = json.loads(self.datasets.info(self.dataset_id))
        self.assertTrue(isinstance(result, dict))

    def _test_get_summary(self):
        result = json.loads(self.datasets.summary(
            self.dataset_id,
            select=self.datasets.SELECT_ALL_FOR_SUMMARY))
        self.assertTrue(isinstance(result, dict))

    def _test_get_summary_with_group(self, group):
        result = json.loads(self.datasets.summary(
            self.dataset_id, group=group,
            select=self.datasets.SELECT_ALL_FOR_SUMMARY))
        self.assertTrue(isinstance(result, dict))
