import simplejson as json

from bamboo.lib.query_args import QueryArgs
from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasets(TestAbstractDatasets):

    def setUp(self):
        TestAbstractDatasets.setUp(self)
        self.dataset_id = self._post_file('water_points.csv')
        self.dataset = Dataset.find_one(self.dataset_id)

    def __test_result(self, result, dframe):
        for column in dframe.columns:
            for i, v in dframe[column].iteritems():
                v = str(v)[0:5]
                self.assertTrue(v in result, v)

    def test_plot(self):
        result = self.controller.plot(self.dataset_id)

        dframe = self.dataset.dframe(
            query_args=QueryArgs(select=self.dataset.schema.numerics_select))
        dframe = dframe.dropna()

        self.__test_result(result, dframe)

    def test_plot_select_none(self):
        column = 'acreage'
        select = json.dumps({column: 1})
        result = self.controller.plot(self.dataset_id, select=select)
        result = json.loads(result)

        self.assertTrue(column in result[self.controller.ERROR])

    def test_plot_select(self):
        column = 'community_pop'
        select = {column: 1}
        result = self.controller.plot(self.dataset_id,
                                      select=json.dumps(select))

        dframe = self.dataset.dframe(QueryArgs(select=select))
        self.__test_result(result, dframe)

    def test_plot_index(self):
        dataset_id = self._post_file()
        dataset = Dataset.find_one(dataset_id)

        column = 'amount'
        select = {column: 1}
        result = self.controller.plot(dataset_id, select=json.dumps(select),
                                      index='submit_date')
        dframe = dataset.dframe()

        dframe = self.dataset.dframe(QueryArgs(select=select))
        self.__test_result(result, dframe)

    def test_plot_type(self):
        column = 'community_pop'
        select = json.dumps({column: 1})
        plot_types = ['area', 'bar', 'line', 'scatterplot', 'stack']

        for plot_type in plot_types:
            result = self.controller.plot(self.dataset_id, select=select,
                                          plot_type=plot_type)
            dframe = self.dataset.dframe()

            for i, amount in dframe[column].iteritems():
                self.assertTrue(str(amount) in result)

    def test_plot_invalid_group(self):
        column = 'bongo'
        result = self.controller.plot(self.dataset_id, group=column)
        result = json.loads(result)

        self.assertTrue(column in result[self.controller.ERROR])

    def test_plot_invalid_index(self):
        column = 'bongo'
        result = self.controller.plot(self.dataset_id, index=column)
        result = json.loads(result)

        self.assertTrue(column in result[self.controller.ERROR])

    def test_plot_output_vega(self):
        result = self.controller.plot(self.dataset_id, vega=True)
        result = json.loads(result)

        self.assertTrue(isinstance(result, dict))

    def test_plot_output_height_width(self):
        result = self.controller.plot(self.dataset_id, height=200, width=300)
        result = result

        self.assertTrue('200' in result)
        self.assertTrue('300' in result)
