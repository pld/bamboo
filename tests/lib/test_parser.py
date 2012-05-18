from tests.test_base import TestBase

from lib.exceptions import ParseError
from lib.parser import Parser


class TestParser(TestBase):

    def setUp(self):
        self.parser = Parser()
        self.row = {'VAR': 1}
        TestBase.setUp(self)

    def _check_func(self, parse_result):
        func, expr_stack = parse_result
        self.assertEqual(func.func_name, 'formula_function')
        self.assertTrue(isinstance(expr_stack, list))
        self.assertTrue(len(expr_stack) > 0)
        return parse_result

    def test_parse_formula(self):
        func, expr_stack = self._check_func(
                self.parser.parse_formula('VAR'))
        self.assertEqual(func(self.row, expr_stack), 1)

    def test_bnf(self):
        result = self.parser.BNF()
        print type(self.parser.bnf)
        self.assertNotEqual(self.parser.bnf, None)

    def test_parse_formula_with_var(self):
        func, expr_stack = self._check_func(
                self.parser.parse_formula('VAR + 1'))
        self.assertEqual(func(self.row, expr_stack), 2)

    def test_parse_formula_bad_formula(self):
        self.assertRaises(ParseError, self.parser.parse_formula, '=BAD +++ FORMULA')
