from tests.test_base import TestBase

from lib.tasks.calculator import calculate_column
from models.dataset import Dataset
from models.observation import Observation


class TestCalculator(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset.save(self.dataset_id)
        Observation.save(self.data, self.dataset)
        self.calculations = [
            'rating',
            'gps',
        ]

    def test_calculator(self):
        dframe = Observation.find(self.dataset, as_df=True)

        for idx, formula in enumerate(self.calculations):
            name = 'test-%s' % idx
            task = calculate_column.delay(self.dataset, dframe,
                    formula, name)

            # test that task has completed
            self.assertTrue(task.ready())
            self.assertTrue(task.successful())

            # test that updated dataframe persisted
            dframe = Observation.find(self.dataset, as_df=True)
            self.assertTrue(name in dframe.columns)

            # test result of calculation
            for idx, row in dframe.iterrows():
                self.assertEqual(row[name], row[formula])
