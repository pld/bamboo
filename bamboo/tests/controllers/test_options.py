from bamboo.controllers.abstract_controller import AbstractController
from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.tests.test_base import TestBase


class TestOptions(TestBase):
    # TODO: check response code == 204
    # and headers are set correctly...

    def setUp(self):
        TestBase.setUp(self)
        self.controller = AbstractController()

    def _test_options(self, response):
        self.assertTrue(response == '')

    def test_abstract_controller_options(self):
        response = self.controller.options()

    def test_dataset_controller_options(self):
        controller = Datasets()
        response = controller.options('dataset_id')

    def test_calculations_controller_options(self):
        controller = Calculations()
        response = self.controller.options('dataset_id')
