from bamboo.lib.datetools import recognize_dates
from bamboo.lib.jsontools import JSONError
from bamboo.models.dataset import Dataset
from bamboo.models.observation import Observation
from bamboo.tests.test_base import TestBase


class TestObservation(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset()
        self.dataset.save(self.test_dataset_ids['good_eats.csv'])

    def _save_records(self):
        Observation().save(self.get_data('good_eats.csv'),
                           self.dataset)
        records = Observation.find(self.dataset)
        self.assertTrue(isinstance(records, list))
        self.assertTrue(isinstance(records[0], dict))
        self.assertTrue('_id' in records[0].keys())
        return records

    def _save_observations(self):
        return Observation().save(
            recognize_dates(self.get_data('good_eats.csv')), self.dataset)

    def test_save(self):
        records = self._save_records()
        self.assertEqual(len(records), 19)

    def test_save_over_bulk(self):
        Observation().save(self.get_data('good_eats_large.csv'),
                           self.dataset)
        records = Observation.find(self.dataset)
        self.assertEqual(len(records), 1001)

    def test_find(self):
        self._save_observations()
        rows = Observation.find(self.dataset)
        self.assertTrue(isinstance(rows, list))

    def test_find_with_query(self):
        self._save_observations()
        rows = Observation.find(self.dataset, '{"rating": "delectible"}')
        self.assertTrue(isinstance(rows, list))

    def test_find_with_bad_query_json(self):
        self._save_observations()
        self.assertRaises(JSONError, Observation.find, self.dataset,
                          '{rating: "delectible"}')

    def test_find_with_select(self):
        self._save_observations()
        rows = Observation.find(self.dataset, select='{"rating": 1}')
        self.assertTrue(isinstance(rows, list))
        self.assertEquals(sorted(rows[0].keys()), ['_id', 'rating'])

    def test_find_with_bad_select(self):
        self._save_observations()
        self.assertRaises(JSONError, Observation.find, self.dataset,
                          select='{rating: 1}')

    def test_find_with_select_and_query(self):
        self._save_observations()
        rows = Observation.find(self.dataset, '{"rating": "delectible"}',
                                '{"rating": 1}')
        self.assertTrue(isinstance(rows, list))
        self.assertEquals(sorted(rows[0].keys()), ['_id', 'rating'])

    def test_delete(self):
        self._save_observations()
        records = Observation.find(self.dataset)
        self.assertNotEqual(records, [])
        Observation.delete_all(self.dataset)
        records = [x for x in Observation.find(self.dataset)]
        self.assertEqual(records, [])
