from bamboo.core.parser import Parser
from bamboo.core.calculator import Calculator
from bamboo.lib.datetools import recognize_dates
from bamboo.models.dataset import Dataset
from bamboo.tests.test_base import TestBase


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset()
        self.dataset.save(
            self.test_dataset_ids['good_eats_with_calculations.csv'])
        dframe = recognize_dates(
            self.get_data('good_eats_with_calculations.csv'))
        self.dataset.save_observations(dframe)
        self.group = None
        self.parser = Parser(self.dataset)
        self.places = 5

    def _equal_msg(self, calculated, stored, formula):
        return '(calculated %s) %s != (stored %s) %s ' % (type(calculated),
               calculated, type(stored), stored) +\
            '(within %s places), formula: %s' % (self.places, formula)

    def _test_calculator(self):
        self.dframe = self.dataset.dframe()
        row = self.dframe.irow(0)

        columns = self.dframe.columns.tolist()
        self.start_num_cols = len(columns)
        self.added_num_cols = 0

        column_labels_to_slugs = {
            column_attrs[Dataset.LABEL]: (column_name) for
            (column_name, column_attrs) in self.dataset.schema.items()
        }
        self.label_list, self.slugified_key_list = [
            list(ary) for ary in zip(*column_labels_to_slugs.items())
        ]

        for idx, formula in enumerate(self.calculations):
            name = 'test-%s' % idx
            self.parser.validate_formula(formula, row)

            calculator = Calculator(self.dataset)

            groups = self.dataset.split_groups(self.group)
            calculator.calculate_column(formula, name, groups)

            self.column_labels_to_slugs = self.dataset.schema.labels_to_slugs

            self._test_calculation_results(name, formula)
