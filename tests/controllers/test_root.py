from cherrypy import HTTPRedirect

from controllers.root import Root
from tests.test_base import TestBase


class TestRoot(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Root()

    def test_GET(self):
        try:
            response = self.controller.GET()
        except HTTPRedirect as redirect:
            self.assertEqual(redirect.status, 303)
            self.assertTrue(redirect.urls[0].endswith('/index.html'))
