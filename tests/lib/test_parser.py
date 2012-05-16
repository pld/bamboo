from tests.test_base import TestBase

from lib.exceptions import ParseError
from lib.parser import Parser


class TestParser(TestBase):

    def setUp(self):
        self.parser = Parser()
        TestBase.setUp(self)

    def test_parse_formula(self):
        func, var_stack = self.parser.parse_formula('GOODFORMULA')
        self.assertEqual(func.func_name, 'formula_function')
        self.assertTrue(isinstance(var_stack, list))
        self.assertTrue(len(var_stack) > 0)

    def test_parse_formula_bad_formula(self):
        self.assertRaises(ParseError, self.parser.parse_formula, '=BAD +++ FORMULA')
