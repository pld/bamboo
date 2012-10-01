from bamboo.lib.io import open_data_file
from bamboo.tests.test_base import TestBase


class TestParser(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def test_open_data_file_no_match(self):
        result = open_data_file('', allow_local_file=True)
        self.assertEqual(result, None)
