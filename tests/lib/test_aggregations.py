from test_calculator import TestCalculator


class TestAggregations(TestCalculator):

    def setUp(self):
        TestCalculator.setUp(self)
        self.calculations = [
            'sum(amount)',
        ]

    def _test_calculation_results(self, name, formula):
        pass

    def test_calculator_with_delay(self):
        self._test_calculator()

    def test_calculator_without_delay(self):
        self._test_calculator(delay=False)

    def test_calculator_with_group(self):
        self.group = 'food_type'
        self._test_calculator(delay=False)
