from bamboo.lib.utils import recognize_dates
from bamboo.tests.test_base import TestBase


class TestUtils(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.test_data['soil_samples.csv']

    def test_recognize_dates(self):
        with_dates = recognize_dates(self.dframe)
        for field in with_dates['single_letter']:
            self.assertTrue(isinstance(field, basestring))
