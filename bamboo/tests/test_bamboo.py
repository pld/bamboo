from subprocess import call

from bamboo.tests.test_base import TestBase
from bamboo import bambooapp  # nopep8


class TestBamboo(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def test_bambooapp(self):
        # this tests that importing bamboo.bambooapp succeeds
        pass

    def test_pep8(self):
        result = call(['pep8', '.'])
        self.assertEqual(result, 0, "Code is not pep8.")
