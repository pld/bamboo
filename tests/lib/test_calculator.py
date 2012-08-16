from tests.test_base import TestBase

from lib.parser import Parser
from lib.tasks.calculator import calculate_column
from lib.utils import build_labels_to_slugs, slugify_columns
from models.dataset import Dataset
from models.observation import Observation


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(
            self.test_dataset_ids['good_eats_with_calculations.csv'])
        dframe = self.test_data['good_eats_with_calculations.csv']
        Observation.save(dframe, self.dataset)
        self.group = None
        self.parser = Parser()
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

        column_labels_to_slugs = build_labels_to_slugs(self.dataset)
        self.label_list, self.slugified_key_list = [
            list(ary) for ary in zip(*column_labels_to_slugs.items())
        ]

        for idx, formula in enumerate(self.calculations):
            name = 'test-%s' % idx
            self.parser.validate_formula(formula, row)

            if delay:
                task = calculate_column.delay(self.parser, self.dataset,
                                              self.dframe, formula, name,
                                              self.group)
                # test that task has completed
                self.assertTrue(task.ready())
                self.assertTrue(task.successful())
            else:
                task = calculate_column(self.parser, self.dataset, self.dframe,
                                        formula, name, self.group)

            self.column_labels_to_slugs = build_labels_to_slugs(self.dataset)

            self._test_calculation_results(name, formula)
