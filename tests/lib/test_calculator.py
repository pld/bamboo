from tests.test_base import TestBase

from lib.constants import DATASET_ID
from models.calculation import Calculation
from models.dataset import Dataset
from models.observation import Observation


class TwistedEnabledTest(object):
    def setup_data_for_calculations(self):
        self.dataset = Dataset.save(self.dataset_id)
        Observation.save(self.data, self.dataset)
        self.formula = 'x + y'
        self.name = 'test'
        record = {
            DATASET_ID: self.dataset[DATASET_ID],
            Calculation.FORMULA: self.formula,
            Calculation.NAME: self.name,
        }
        self.calculation_id = str(Calculation.collection.insert(record))


class TestRemoteCalculator(TestBase, TwistedEnabledTest):

    def setUp(self):
        TestBase.setUp(self)
        self.setup_server()
        self.setup_data_for_calculations()

    def test_remote_calculator(self):
        self.proto.dataReceived('%s\r\n' % self.calculation_id)
        self.assertEqual(self.tr.value().strip(), '1')

        # TODO test result of calculation
#        def func(calculator_id):
#            dframe = Observation.find(self.dataset, as_df=True)
#            self.assertTrue(self.name in dframe.columns)
#            for key, value in dframe[self.name].iteritems():
#                self.assertEqual(value, self.formula)


class TestCalculatorClient(TestBase, TwistedEnabledTest):

    def setUp(self):
        TestBase.setUp(self)
        self.setup_client()
        self.setup_data_for_calculations()

    def test_calculator_client(self):
        d = self.proto.send(self.calculation_id)
        self.assertEqual(self.tr.value(), '%s\r\n' % self.calculation_id)
        self.tr.clear()

        d.addCallback(self.assertEqual, '1')
        self.proto.dataReceived('1\r\n')
        return d
