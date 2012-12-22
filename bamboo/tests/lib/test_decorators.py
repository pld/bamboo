from bamboo.tests import decorators as test_decorators
from bamboo.tests.test_base import TestBase


class TestDecorators(TestBase):

    def setUp(self):
        def test_func():
            pass
        self._test_func = test_func

    def _test_decorator(self, func):
        wrapped_test_func = func(self._test_func)
        self.assertTrue(hasattr(wrapped_test_func, '__call__'))
        wrapped_test_func()

    def test_print_time(self):
        self._test_decorator(test_decorators.print_time)
