import json

from cherrypy import HTTPRedirect

from bamboo.controllers.version import Version
from bamboo.tests.test_base import TestBase


class TestVersion(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Version()

    def test_index(self):
        response = json.loads(self.controller.index())
        self.assertTrue('version' in response.keys())
