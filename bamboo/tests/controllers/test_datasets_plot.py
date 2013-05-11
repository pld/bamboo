import simplejson as json

from bamboo.lib.query_args import QueryArgs
from bamboo.models.dataset import Dataset
from bamboo.tests.controllers.test_abstract_datasets import\
    TestAbstractDatasets


class TestDatasets(TestAbstractDatasets):

    def setUp(self):
        TestAbstractDatasets.setUp(self)
        self.dataset_id = self._post_file()
        self.dataset = Dataset.find_one(self.dataset_id)

    def test_plot(self):
        result = self.controller.plot(self.dataset_id)

        dframe = self.dataset.dframe(
            query_args=QueryArgs(select=self.dataset.schema.numerics_select))
        dframe = dframe.dropna()

        for column in dframe.columns:
            for i, amount in dframe[column].iteritems():
                self.assertTrue(str(amount) in result, amount)

    def test_plot_select(self):
        column = 'amount'
        select = json.dumps({column: 1})
        result = self.controller.plot(self.dataset_id, select=select)
        dframe = self.dataset.dframe()

        for i, amount in dframe[column].iteritems():
            self.assertTrue(str(amount) in result)

    def test_plot_index(self):
        column = 'amount'
        select = json.dumps({column: 1})
        result = self.controller.plot(self.dataset_id, select=select,
                                      index='submit_date')
        dframe = self.dataset.dframe()

        for i, amount in dframe[column].iteritems():
            self.assertTrue(str(amount) in result)

    def test_plot_type(self):
        column = 'amount'
        select = json.dumps({column: 1})
        plot_types = ['area', 'bar', 'line', 'scatterplot', 'stack']

        for plot_type in plot_types:
            result = self.controller.plot(self.dataset_id, select=select,
                                          plot_type=plot_type)
            dframe = self.dataset.dframe()

            for i, amount in dframe[column].iteritems():
                self.assertTrue(str(amount) in result)
