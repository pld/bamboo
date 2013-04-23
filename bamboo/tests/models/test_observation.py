from bamboo.core.frame import INDEX
from bamboo.lib.mongo import dump_mongo_json, MONGO_ID, MONGO_ID_ENCODED
from bamboo.lib.query_args import QueryArgs
from bamboo.models.dataset import Dataset
from bamboo.models.observation import Observation
from bamboo.tests.test_base import TestBase


class TestObservation(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dataset = Dataset()
        self.dataset.save(self.test_dataset_ids['good_eats.csv'])
        self.query_args = QueryArgs({"rating": "delectible"})

    def __save_records(self):
        Observation.save(self.get_data('good_eats.csv'),
                         self.dataset)
        records = Observation.find(self.dataset)
        self.assertTrue(isinstance(records, list))
        self.assertTrue(isinstance(records[0], dict))
        self.assertTrue('_id' in records[0].keys())

        return records

    def __decode(self, row):
        return Observation.encode(row,
                                  encoding=Observation.decoding(self.dataset))

    def test_encoding(self):
        self.__save_records()
        encoding = Observation.encoding(self.dataset)

        for column in self.dataset.dframe().columns:
            if column == MONGO_ID:
                column = MONGO_ID_ENCODED

            self.assertTrue(column in encoding.keys())

        for v in encoding.values():
            self.assertTrue(isinstance(int(v), int))

    def test_encode_no_dataset(self):
        records = self.__save_records()

        for record in records:
            encoded = Observation.encode(record)
            self.assertEqual(dump_mongo_json(encoded), dump_mongo_json(record))

    def test_save(self):
        records = self.__save_records()
        self.assertEqual(len(records), 19)

    def test_save_over_bulk(self):
        Observation.save(self.get_data('good_eats_large.csv'),
                         self.dataset)
        records = Observation.find(self.dataset)

        self.assertEqual(len(records), 1001)

    def test_find(self):
        self.__save_records()
        rows = Observation.find(self.dataset)

        self.assertTrue(isinstance(rows, list))

    def test_find_with_query(self):
        self.__save_records()
        rows = Observation.find(self.dataset, self.query_args)

        self.assertTrue(isinstance(rows, list))

    def test_find_with_select(self):
        self.__save_records()
        query_args = QueryArgs(select={"rating": 1})
        rows = Observation.find(self.dataset, query_args)

        self.assertTrue(isinstance(rows, list))

        row = self.__decode(rows[0])

        self.assertEquals(sorted(row.keys()), ['_id', 'rating'])

    def test_find_with_select_and_query(self):
        self.__save_records()
        self.query_args.select = {"rating": 1}
        rows = Observation.find(self.dataset, self.query_args)
        self.assertTrue(isinstance(rows, list))

        row = self.__decode(rows[0])

        self.assertEquals(sorted(row.keys()), ['_id', 'rating'])

    def test_delete_all(self):
        self.__save_records()
        records = Observation.find(self.dataset)
        self.assertNotEqual(records, [])
        Observation.delete_all(self.dataset)
        records = Observation.find(self.dataset)

        self.assertEqual(records, [])

    def test_delete_one(self):
        self.__save_records()
        records = Observation.find(self.dataset)
        self.assertNotEqual(records, [])

        row = self.__decode(records[0])

        Observation.delete(self.dataset, row[INDEX])
        new_records = Observation.find(self.dataset)

        # Dump to avoid problems with nan != nan.
        self.assertEqual(dump_mongo_json(records[1:]),
                         dump_mongo_json(new_records))

    def test_delete_encoding(self):
        self.__save_records()
        encoding = Observation.encoding(self.dataset)

        self.assertTrue(isinstance(encoding, dict))

        Observation.delete_encoding(self.dataset)
        encoding = Observation.encoding(self.dataset)

        self.assertEqual(encoding, None)
