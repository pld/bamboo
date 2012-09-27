from tests.test_base import TestBase

from lib.parser import Parser
from lib.tasks.calculator import Calculator
from lib.utils import recognize_dates
from models.dataset import Dataset
from models.observation import Observation


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset()
        self.dataset.save(
            self.test_dataset_ids['good_eats_with_calculations.csv'])
        dframe = recognize_dates(
            self.test_data['good_eats_with_calculations.csv'])
        Observation().save(dframe, self.dataset)
        self.group = None
        self.parser = Parser(self.dataset.record)
        self.places = 5

    def _equal_msg(self, calculated, stored, formula):
        return '(calculated %s) %s != (stored %s) %s ' % (type(calculated),
               calculated, type(stored), stored) +\
            '(within %s places), formula: %s' % (self.places, formula)

    def _test_calculator(self, delay=True):
        self.dframe = Observation.find(self.dataset, as_df=True)
        row = self.dframe.irow(0)

        columns = self.dframe.columns.tolist()
        self.start_num_cols = len(columns)
        self.added_num_cols = 0

        column_labels_to_slugs = dict([
            (column_attrs[Dataset.LABEL], (column_name)) for
            (column_name, column_attrs) in self.dataset.data_schema.items()])
        self.label_list, self.slugified_key_list = [
            list(ary) for ary in zip(*column_labels_to_slugs.items())
        ]

        for idx, formula in enumerate(self.calculations):
            name = 'test-%s' % idx
            self.parser.validate_formula(formula, row)

            calculator = Calculator(self.dataset)

            if delay:
                task = calculator.calculate_column.delay(
                    calculator, formula, name, self.group)
                # test that task has completed
                self.assertTrue(task.ready())
                self.assertTrue(task.successful())
            else:
                task = calculator.calculate_column(
                    calculator, formula, name, self.group)

            self.column_labels_to_slugs = self.dataset.build_labels_to_slugs()

            self._test_calculation_results(name, formula)
