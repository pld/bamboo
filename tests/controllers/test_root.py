from controllers.root import Root
from tests.test_base import TestBase

class TestRoot(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Root()

    def test_GET(self):
        response = self.controller.GET()
        self.assertEqual(response, 'Ohai World!')
