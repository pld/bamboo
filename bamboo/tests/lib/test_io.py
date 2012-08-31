from tests.test_base import TestBase

from lib.io import open_data_file


class TestParser(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def test_open_data_file_no_match(self):
        result = open_data_file('', allow_local_file=True)
        self.assertEqual(result, None)
