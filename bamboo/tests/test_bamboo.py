from subprocess import Popen

from tests.test_base import TestBase


class TestBamboo(TestBase):

    def test_import_server(self):
        import run_server

    def test_start_server(self):
        process = Popen(['python', 'run_server.py'])
        process.terminate()
