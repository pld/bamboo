from tests.test_base import TestBase

from lib.exceptions import ParseError
from lib.parser import Parser


class TestParser(TestBase):

    def setUp(self):
        self.parser = Parser()
        self.row = {'VAR': 1}
        TestBase.setUp(self)

    def _check_func(self, parse_result):
        func = parse_result
        self.assertEqual(func.func_name, '_eval')
        return parse_result

    def test_parse_formula(self):
        func = self._check_func(
                self.parser.parse_formula('VAR'))
        self.assertEqual(func(self.row, self.parser), 1)

    def test_bnf(self):
        result = self.parser.BNF()
        print type(self.parser.bnf)
        self.assertNotEqual(self.parser.bnf, None)

    def test_parse_formula_with_var(self):
        func = self._check_func(
                self.parser.parse_formula('VAR + 1'))
        self.assertEqual(func(self.row, self.parser), 2)

    def test_parse_formula_bad_formula(self):
        bad_formulas = [
            '=BAD +++ FOR',
            #'2 +>+ 1',
            #'1 ** 2',
        ]

        for bad_formula in bad_formulas:
            self.assertRaises(ParseError, self.parser.parse_formula,
                    bad_formula)
