from datetime import datetime

from bamboo.lib.datetools import recognize_dates
from bamboo.lib.schema_builder import DATETIME, SIMPLETYPE, Schema
from bamboo.tests.test_base import TestBase


class TestDatetools(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.dframe = self.get_data('good_eats.csv')

    def test_recognize_dates(self):
        dframe = self.get_data('soil_samples.csv')
        with_dates = recognize_dates(dframe)

        for field in with_dates['single_letter']:
            self.assertTrue(isinstance(field, basestring))

    def test_recognize_dates_as_dates(self):
        df_with_dates = recognize_dates(self.dframe)

        for field in df_with_dates['submit_date']:
            self.assertTrue(isinstance(field, datetime))

    def test_recognize_dates_from_schema(self):
        schema = Schema({
            'submit_date': {
                SIMPLETYPE: DATETIME
            }
        })
        df_with_dates = recognize_dates(self.dframe, schema)

        for field in df_with_dates['submit_date']:
            self.assertTrue(isinstance(field, datetime))
