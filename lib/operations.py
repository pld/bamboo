# future must be first
from __future__ import division
import operator

import numpy as np


class EvalTerm(object):
    """
    Base class for evaluation
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def operator_operands(self, tokenlist):
        "generator to extract operators and operands in pairs"
        it = iter(tokenlist)
        while 1:
            try:
                yield (it.next(), it.next())
            except StopIteration:
                break

    def operation(self, op, result, val):
        return self.operations[op](result, val)


class EvalConstant(EvalTerm):
    """
    Class to evaluate a parsed constant or variable
    """

    def _eval(self, row):
        try:
            np.float64(self.value)
            return self.value
        except ValueError:
            return row[self.value]


class EvalString(EvalTerm):
    """
    Class to evaluate a parsed string.
    """

    def _eval(self, row):
        return self.value


class EvalSignOp(EvalTerm):
    """
    Class to evaluate expressions with a leading + or - sign
    """

    def __init__(self, tokens):
        self.sign, self.value = tokens[0]

    def _eval(self, row):
        mult = {'+': 1, '-': -1}[self.sign]
        return mult * self.value._eval(row)


class EvalBinaryArithOp(EvalTerm):
    """
    Class for evaluating binary arithmetic operations
    """

    operations = {
        '+': operator.__add__,
        '-': operator.__sub__,
        '*': operator.__mul__,
        '/': operator.__truediv__,
        '^': operator.__pow__,
    }

    def _eval(self, row):
        result = np.float64(self.value[0]._eval(row))
        for op, val in self.operator_operands(self.value[1:]):
            val = np.float64(val._eval(row))
            result = self.operation(op, result, val)
            if np.isinf(result):
                return np.nan
        return result


class EvalMultOp(EvalBinaryArithOp):
    """
    Class to distinguish precedence of multiplication/division expressions
    """
    pass


class EvalPlusOp(EvalBinaryArithOp):
    """
    Class to distinguish precedence of addition/subtraction expressions
    """
    pass


class EvalExpOp(EvalBinaryArithOp):
    """
    Class to distinguish precedence of exponentiation expressions
    """
    pass


class EvalComparisonOp(EvalTerm):
    """
    Class to evaluate comparison expressions
    """

    opMap = {
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "!=": lambda a, b: a != b,
        "=": lambda a, b: a == b,
    }

    def _eval(self, row):
        val1 = np.float64(self.value[0]._eval(row))
        for op, val in self.operator_operands(self.value[1:]):
            fn = EvalComparisonOp.opMap[op]
            val2 = np.float64(val._eval(row))
            if not fn(val1, val2):
                break
            val1 = val2
        else:
            return True
        return False


class EvalNotOp(EvalTerm):
    """
    Class to evaluate not expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0][1]

    def _eval(self, row):
        return not self.value._eval(row)


class EvalBinaryBooleanOp(EvalTerm):
    """
    Class for evaluating binary boolean operations
    """

    operations = {
        'and': lambda p, q: p and q,
        'or': lambda p, q: p or q,
    }

    def _eval(self, row):
        result = np.bool_(self.value[0]._eval(row))
        for op, val in self.operator_operands(self.value[1:]):
                val = np.bool_(val._eval(row))
                result = self.operation(op, result, val)
        return result


class EvalAndOp(EvalBinaryBooleanOp):
    """
    Class to distinguish precedence of and expressions
    """
    pass


class EvalOrOp(EvalBinaryBooleanOp):
    """
    Class to distinguish precedence of or expressions
    """
    pass


class EvalInOp(EvalTerm):
    """
    Class to eval in expressions.
    """

    def _eval(self, row):
        val_to_test = str(self.value[0]._eval(row))
        val_list = []
        for op, val in self.operator_operands(self.value[1:]):
            val_list.append(val._eval(row))
        return val_to_test in val_list
