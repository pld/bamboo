from tests.test_base import TestBase

from lib.exceptions import ParseError
from lib.parser import parse_formula


class TestParser(TestBase):

    def setUp(self):
        TestBase.setUp(self)

    def test_parse_formula(self):
        func, var_stack = parse_formula('GOODFORMULA')
        self.assertEqual(func.func_name, 'formula_function')
        self.assertTrue(isinstance(var_stack, list))
        self.assertTrue(len(var_stack) > 0)

    def test_parse_formula_bad_formula(self):
        self.assertRaises(ParseError, parse_formula, '=BAD +++ FORMULA')
