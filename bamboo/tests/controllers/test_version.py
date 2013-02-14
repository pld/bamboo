import json

from bamboo.controllers.version import Version
from bamboo.tests.test_base import TestBase


class TestVersion(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Version()

    def test_index(self):
        response = json.loads(self.controller.index())
        response_keys = response.keys()
        keys = [
            'version',
            'description',
            'branch',
            'commit',
        ]
        for key in keys:
            self.assertTrue(key in response_keys)
