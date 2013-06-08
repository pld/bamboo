import cherrypy

from bamboo.controllers.abstract_controller import AbstractController
from bamboo.tests.test_base import TestBase


class TestOptions(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = AbstractController()

    def test_options_empty_response(self):
        response = self.controller.options()
        self.assertTrue(response == '')

    def test_options_status_code(self):
        self.controller.options()
        self.assertEqual(
            cherrypy.response.status, self.controller.NO_CONTENT_STATUS_CODE)

    def test_options_content_length(self):
        self.controller.options()
        self.assertEqual(cherrypy.response.headers['Content-Length'], 0)
