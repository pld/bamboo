from bamboo.lib.datetools import recognize_dates
from bamboo.tests.test_base import TestBase


class TestDatetools(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.get_data('soil_samples.csv')

    def test_recognize_dates(self):
        with_dates = recognize_dates(self.dframe)
        for field in with_dates['single_letter']:
            self.assertTrue(isinstance(field, basestring))
