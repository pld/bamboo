import unittest

from decorators import requires_internet


class TestBase(unittest.TestCase):

    def setUp(self):
        self._load_test_data()

    @requires_internet
    def _load_test_data(self):
        pass
