#import lib.decorators
from lib import decorators
from tests.test_base import TestBase

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
        self._test_decorator(decorators.print_time)

    def test_requires_internet(self):
        self._test_decorator(decorators.requires_internet)

    def test_requires_internet_bad_url(self):
        wrapped_test_func = decorators.requires_internet(self._test_func,
                'http://')
        self.assertTrue(hasattr(wrapped_test_func, '__call__'))
        self.assertRaises(AssertionError, wrapped_test_func)

    def test_check_bad_url(self):
        result = decorators._check_url('http://')
        self.assertEqual(result, False)
