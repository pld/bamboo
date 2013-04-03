from bamboo.core.parser import ParseError, Parser
from bamboo.models.dataset import Dataset
from bamboo.tests.test_base import TestBase


class TestParser(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset_id = self._post_file()
        self.dataset = Dataset.find_one(self.dataset_id)
        self.parser = Parser(self.dataset)
        self.row = {'amount': 1}

    def _parse_and_check_func(self, formula):
        functions, _ = self.parser.parse_formula(formula)
        for func in functions:
            self.assertEqual(func.func.func_name, 'eval')
        return functions[0]

    def test_parse_formula(self):
        func = self._parse_and_check_func('amount')
        self.assertEqual(func(self.row, self.parser.context), 1)

    def test_bnf(self):
        self.parser._Parser__build_bnf()
        self.assertNotEqual(self.parser.bnf, None)

    def test_parse_formula_with_var(self):
        func = self._parse_and_check_func('amount + 1')
        self.assertEqual(func(self.row, self.parser.context), 2)

    def test_parse_formula_dependent_columns(self):
        test_formulae = {
            '1': set([]),
            'amount + 1': set(['amount']),
            'amount + gps_alt': set(['amount', 'gps_alt']),
            # TODO add aggregations?
        }
        for formula, column_list in test_formulae.iteritems():
            functions, dependent_columns = self.parser.parse_formula(formula)
            self.assertEqual(column_list, dependent_columns)

    def test_parse_formula_bad_formula(self):
        bad_formulas = [
            '=BAD +++ FOR',
            '2 +>+ 1',
            '1 ** 2',
        ]

        for bad_formula in bad_formulas:
            self.assertRaises(ParseError, self.parser.parse_formula,
                              bad_formula)
