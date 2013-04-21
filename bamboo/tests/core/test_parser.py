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
        self.parser = Parser(self.dataset)
        self.row = {'amount': 1}

    def _parse_and_check_func(self, formula):
        functions, _ = self.parser.parse_formula(formula)
        for func in functions:
            self.assertEqual(func.func.func_name, 'eval')
        return functions[0]

    def test_parse_formula(self):
        func = self._parse_and_check_func('amount')
        self.assertEqual(func(self.row, self.parser.dataset), 1)

    def test_bnf(self):
        self.parser._Parser__build_bnf()
        self.assertNotEqual(self.parser.bnf, None)

    def test_parse_formula_with_var(self):
        func = self._parse_and_check_func('amount + 1')
        self.assertEqual(func(self.row, self.parser.dataset), 2)

    def test_parse_formula_dependent_columns(self):
        formulas_to_deps = combine_dicts(AGG_CALCS_TO_DEPS, CALCS_TO_DEPS)

        for formula, column_list in formulas_to_deps.iteritems():
            functions, dependent_columns = self.parser.parse_formula(formula)
            self.assertEqual(set(column_list), dependent_columns)

    def test_parse_formula_bad_formula(self):
        bad_formulas = [
            '=BAD +++ FOR',
            '2 +>+ 1',
            '1 ** 2',
        ]

        for bad_formula in bad_formulas:
            self.assertRaises(ParseError, self.parser.parse_formula,
                              bad_formula)
