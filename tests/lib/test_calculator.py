from tests.test_base import TestBase

from lib.calculator import Calculator
from models.dataset import Dataset
from models.observation import Observation


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.dataset_id)
        Observation.save(self.data, self.dataset)
        self._load_calculation()

    def test_calculator(self):
        calculator = Calculator()
        dframe = Observation.find(self.dataset, as_df=True)
        task = calculator.run.delay(self.dataset, dframe,
                self.formula, self.name)
        dframe = task.get()
        self.assertTrue(task.ready())
        self.assertTrue(task.successful())
        # TODO test result of calculation!
        #dframe = Observation.find(self.dataset, as_df=True)
        self.assertTrue(self.name in dframe.columns)
        for key, value in dframe[self.name].iteritems():
            self.assertEqual(value, self.formula)
