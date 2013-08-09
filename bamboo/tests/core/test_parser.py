from bamboo.core.parser import ParseError, Parser
from bamboo.lib.utils import combine_dicts
from bamboo.models.dataset import Dataset
from bamboo.tests.test_base import TestBase
from bamboo.tests.core.test_aggregations import AGG_CALCS_TO_DEPS
from bamboo.tests.core.test_calculations import CALCS_TO_DEPS


class TestParser(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset_id = self._post_file()
        self.dataset = Dataset.find_one(self.dataset_id)
        self.parser = Parser()
        self.row = {'amount': 1}

    def _parse_and_check_func(self, formula):
        functions = Parser.parse_functions(formula)
        for func in functions:
            self.assertEqual(func.func.func_name, 'eval')
        return functions[0]

    def test_parse_formula(self):
        func = self._parse_and_check_func('amount')
        self.assertEqual(func(self.row, self.dataset), 1)

    def test_bnf(self):
        bnf = self.parser._Parser__build_bnf()
        self.assertNotEqual(bnf, None)

    def test_parse_formula_with_var(self):
        func = self._parse_and_check_func('amount + 1')
        self.assertEqual(func(self.row, self.dataset), 2)

    def test_parse_formula_dependent_columns(self):
        formulas_to_deps = combine_dicts(AGG_CALCS_TO_DEPS, CALCS_TO_DEPS)

        for formula, column_list in formulas_to_deps.iteritems():
            columns = Parser.dependent_columns(formula, self.dataset)
            self.assertEqual(set(column_list), columns)

    def test_parse_formula_bad_formula(self):
        bad_formulas = [
            '=BAD +++ FOR',
            '2 +>+ 1',
            '1 ** 2',
        ]

        for bad_formula in bad_formulas:
            self.assertRaises(ParseError, Parser.parse, bad_formula)
