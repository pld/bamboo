import simplejson as json

from bamboo.controllers.datasets import Datasets
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasetsTsFunctions(TestAbstractDatasets):

    def setUp(self):
        TestAbstractDatasets.setUp(self)

    def __build_resample_result(self, query=None):
        dataset_id = self._post_file('good_eats.csv')
        date_column = 'submit_date'
        interval = 'W'
        results = json.loads(self.controller.resample(
            dataset_id, date_column, interval, query=query))

        self.assertTrue(isinstance(results, list))

        return [date_column, results]

    def __check_interval(self, date_column, results):
        last_date_time = None

        for row in results:
            new_date_time = row[date_column]['$date']

            if last_date_time:
                self.assertEqual(604800000, new_date_time - last_date_time)

            last_date_time = new_date_time

    def test_resample_only_shows_numeric(self):
        date_column, results = self.__build_resample_result()

        permitted_keys = [
            '_id',
            '_percentage_complete',
            'gps_alt',
            'gps_precision',
            'amount',
            'gps_latitude',
            'gps_longitude',
        ] + [date_column]

        for result in results:
            for key in result.keys():
                self.assertTrue(key in permitted_keys)

    def test_resample_interval_correct(self):
        date_column, results = self.__build_resample_result()

        self.__check_interval(date_column, results)

    def test_resample_non_date_column(self):
        dataset_id = self._post_file('good_eats.csv')
        result = json.loads(self.controller.resample(
            dataset_id, 'amount', 'W'))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Datasets.ERROR in result)
        self.assertTrue('DatetimeIndex' in result[Datasets.ERROR])

    def test_resample_bad_interval(self):
        dataset_id = self._post_file('good_eats.csv')
        interval = 'BAD'
        result = json.loads(self.controller.resample(
            dataset_id, 'submit_date', interval))

        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Datasets.ERROR in result)
        self.assertEqual(
            'Could not evaluate %s' % interval, result[Datasets.ERROR])

    def test_resample(self):
        expected_length = 17
        date_column, results = self.__build_resample_result()

        self.assertEqual(expected_length, len(results))

    def test_resample_with_query(self):
        expected_length = 15
        query = '{"food_type": "lunch"}'
        date_column, results = self.__build_resample_result(query)

        self.assertEqual(expected_length, len(results))

        self.__check_interval(date_column, results)

    def test_rolling_mean(self):
        dataset_id = self._post_file('good_eats.csv')
        window = '3'
        results = json.loads(self.controller.rolling(
            dataset_id, window))
        self.assertTrue(isinstance(results, list))

        for i, row in enumerate(results):
            if i < int(window) - 1:
                for value in row.values():
                    self.assertEqual('null', value)
            else:
                self.assertTrue(isinstance(row['amount'], float))

    def test_rolling_bad_window(self):
        dataset_id = self._post_file('good_eats.csv')
        window = '3n'
        results = json.loads(self.controller.rolling(
            dataset_id, window))
        self.assertTrue(Datasets.ERROR in results.keys())

    def test_rolling_bad_type(self):
        dataset_id = self._post_file('good_eats.csv')
        results = json.loads(self.controller.rolling(
            dataset_id, 3, win_type='BAD'))

        self.assertTrue(isinstance(results, dict))
        self.assertTrue(Datasets.ERROR in results)
        self.assertEqual('Unknown window type.', results[Datasets.ERROR])
