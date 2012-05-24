from tests.test_base import TestBase

from lib.io import open_data_file, create_dataset_from_url


class TestParser(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def test_open_data_file_no_match(self):
        result = open_data_file('')
        self.assertEqual(result, None)
