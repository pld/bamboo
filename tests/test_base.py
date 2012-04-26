import unittest

from pandas import read_csv

from utils import open_data_file


class TestBase(unittest.TestCase):

    def setUp(self):
        self._load_test_data()

    def _load_test_data(self):
        f = open_data_file('file://tests/fixtures/good_eats.csv')
        self.data = read_csv(f, na_values=['n/a'])
