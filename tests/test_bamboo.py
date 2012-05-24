from subprocess import Popen

from tests.test_base import TestBase


class TestBamboo(TestBase):

    def test_import_server(self):
        import bamboo

    def test_start_server(self):
        process = Popen(['python', 'bamboo.py'])
        process.terminate()
