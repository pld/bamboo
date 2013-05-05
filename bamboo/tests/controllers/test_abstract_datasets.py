import simplejson as json

from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.core.frame import BambooFrame
from bamboo.core.summary import SUMMARY
from bamboo.lib.mongo import MONGO_ID
from bamboo.models.dataset import Dataset
from bamboo.tests.test_base import TestBase


def comparable(dframe):
    return [reduce_precision(r) for r in BambooFrame(dframe).to_jsondict()]


def reduce_precision(row):
    return {k: round(v, 10) if isinstance(v, float) else v
            for k, v in row.iteritems()}


class TestAbstractDatasets(TestBase):

    NUM_COLS = 15
    NUM_ROWS = 19

    def setUp(self):
        TestBase.setUp(self)
        self.controller = Datasets()
        self._file_name = 'good_eats.csv'
        self._update_file_name = 'good_eats_update.json'
        self._update_check_file_path = '%sgood_eats_update_values.json' % (
            self.FIXTURE_PATH)
        self.default_formulae = [
            'amount',
            'amount + 1',
            'amount - 5',
        ]

    def _put_row_updates(self, dataset_id=None, file_name=None, validate=True):
        if not dataset_id:
            dataset_id = self.dataset_id

        if not file_name:
            file_name = self._update_file_name

        update = open('%s%s' % (self.FIXTURE_PATH, file_name), 'r').read()
        result = json.loads(self.controller.update(dataset_id=dataset_id,
                                                   update=update))

        if validate:
            self.assertTrue(isinstance(result, dict))
            self.assertTrue(Dataset.ID in result)

        # set up the (default) values to test against
        with open(self._update_check_file_path, 'r') as f:
            self._update_values = json.loads(f.read())

    def _load_schema(self):
        return json.loads(
            self.controller.info(self.dataset_id))[Dataset.SCHEMA]

    def _check_dframes_are_equal(self, dframe1, dframe2):
        rows1 = comparable(dframe1)
        rows2 = comparable(dframe2)

        self.__check_dframe_is_subset(rows1, rows2)
        self.__check_dframe_is_subset(rows2, rows1)

    def __check_dframe_is_subset(self, rows1, rows2):
        for row in rows1:
            self.assertTrue(row in rows2,
                            '\nrow:\n%s\n\nnot in rows2:\n%s' % (row, rows2))

    def _post_calculations(self, formulae=[], group=None):
        schema = self._load_schema()
        controller = Calculations()

        for idx, formula in enumerate(formulae):
            name = 'calc_%d' % idx if not schema or\
                formula in schema.keys() else formula

            controller.create(self.dataset_id, formula=formula, name=name,
                              group=group)

    def _test_summary_built(self, result):
        # check that summary is created
        self.assertTrue(isinstance(result, dict))
        self.assertTrue(Dataset.ID in result)
        self.dataset_id = result[Dataset.ID]

        results = self.controller.summary(
            self.dataset_id,
            select=self.controller.SELECT_ALL_FOR_SUMMARY)

        return self._test_summary_results(results)

    def _test_summary_results(self, results):
        results = json.loads(results)
        self.assertTrue(isinstance(results, dict))
        return results

    def _test_aggregations(self, groups=['']):
        results = json.loads(self.controller.aggregations(self.dataset_id))
        self.assertTrue(isinstance(results, dict))
        self.assertEqual(len(results.keys()), len(groups))
        self.assertEqual(results.keys(), groups)
        linked_dataset_id = results[groups[0]]
        self.assertTrue(isinstance(linked_dataset_id, basestring))

        # inspect linked dataset
        return json.loads(self.controller.show(linked_dataset_id))

    def _test_summary_no_group(self, results, dataset_id=None, group=None):
        if not dataset_id:
            dataset_id = self.dataset_id

        group = [group] if group else []
        result_keys = results.keys()

        # minus the column that we are grouping on
        self.assertEqual(len(result_keys), self.NUM_COLS - len(group))

        columns = [col for col in
                   self.get_data(self._file_name).columns.tolist()
                   if not col in [MONGO_ID] + group]

        dataset = Dataset.find_one(dataset_id)
        labels_to_slugs = dataset.schema.labels_to_slugs

        for col in columns:
            slug = labels_to_slugs[col]
            self.assertTrue(slug in result_keys,
                            'col (slug): %s in: %s' % (slug, result_keys))
            self.assertTrue(SUMMARY in results[slug].keys())
