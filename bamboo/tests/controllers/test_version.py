import json

from cherrypy import HTTPRedirect

from bamboo.controllers.version import Version
from bamboo.tests.test_base import TestBase


class TestRoot(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Version()

    def test_GET(self):
        response = json.loads(self.controller.GET())
        self.assertTrue('version' in response.keys())
