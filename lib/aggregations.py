class EvalSumAgg(object):
    """
    Eval sum aggregation.
    """

    def __init__(self, tokens):
        self.value = tokens[0][1]
        self.result = 0

    def _eval(self, row):
        self.result += self.value._eval(row)
        return self.result
