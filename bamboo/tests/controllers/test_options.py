from bamboo.controllers.abstract_controller import AbstractController
from bamboo.tests.test_base import TestBase


class TestOptions(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = AbstractController()

    def test_options(self):
        response = self.controller.options()
        self.assertTrue(response == '')
        # TODO: check response code == 204
        # and headers are set correctly...
