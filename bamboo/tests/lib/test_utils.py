from bamboo.lib.utils import recognize_dates
from bamboo.tests.test_base import TestBase


class TestUtils(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.test_data['soil_samples.csv']

    def test_recognize_dates(self):
        print self.dframe
