from cherrypy import HTTPRedirect

from bamboo.controllers.root import Root
from bamboo.tests.test_base import TestBase


class TestRoot(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Root()

    def test_index(self):
        try:
            response = self.controller.index()
        except HTTPRedirect as redirect:
            self.assertEqual(redirect.status, 303)
            self.assertTrue(redirect.urls[0].endswith('/docs/index.html'))
