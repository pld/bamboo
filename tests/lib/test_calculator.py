from tests.test_base import TestBase

from lib.tasks.calculator import calculate_column
from models.dataset import Dataset
from models.observation import Observation


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.dataset_id)
        Observation.save(self.data, self.dataset)
        self._load_calculation()

    def test_calculator(self):
        dframe = Observation.find(self.dataset, as_df=True)
        task = calculate_column.delay(self.dataset, dframe,
                self.formula, self.name)

        # test that task has completed
        self.assertTrue(task.ready())
        self.assertTrue(task.successful())

        # test that updated dataframe persisted
        dframe = Observation.find(self.dataset, as_df=True)
        self.assertTrue(self.name in dframe.columns)

        # test result of calculation
        for idx, row in dframe.iterrows():
            self.assertEqual(row[self.name], row[self.formula])
