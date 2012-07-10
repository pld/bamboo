from test_calculator import TestCalculator


class TestAggregations(TestCalculator):

    def setUp(self):
        TestCalculator.setUp(self)
        self.calculations = [
            'sum(amount)',
        ]
