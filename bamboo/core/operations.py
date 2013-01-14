# future must be first
from __future__ import division
import operator

import numpy as np
from scipy.stats import percentileofscore

from bamboo.lib.datetools import safe_parse_date_to_unix_time,\
    parse_str_to_unix_time


class EvalTerm(object):
    """Base class for evaluation."""

    def __init__(self, tokens):
        self.tokens = tokens
        self.value = tokens[0]

    def operator_operands(self, tokenlist):
        """Generator to extract operators and operands in pairs."""
        _it = iter(tokenlist)

        while 1:
            try:
                yield (_it.next(), _it.next())
            except StopIteration:
                break

    def operation(self, oper, result, val):
        return self.operations[oper](result, val)


class EvalConstant(EvalTerm):
    """Class to evaluate a parsed constant or variable."""

    def eval(self, row, context):
        try:
            return np.float64(self.value)
        except ValueError:
            # it may be a variable
            field = self.field(row)
            if field:
                # it is a variable, save as dependency
                context.dependent_columns.add(self.value)

            # test is date and parse as date
            return self._parse_field(field, context)

    def _parse_field(self, field, context):
            schema = context.schema

            if schema and schema.is_date_simpletype(self.value):
                field = safe_parse_date_to_unix_time(field)

            return field

    def field(self, row):
        return row.get(self.value)


class EvalString(EvalTerm):
    """Class to evaluate a parsed string."""

    def eval(self, row, context):
        return self.value


class EvalSignOp(EvalTerm):
    """Class to evaluate expressions with a leading + or - sign."""

    def __init__(self, tokens):
        self.sign, self.value = tokens[0]

    def eval(self, row, context):
        mult = {'+': 1, '-': -1}[self.sign]
        return mult * self.value.eval(row, context)


class EvalBinaryArithOp(EvalTerm):
    """Class for evaluating binary arithmetic operations."""

    operations = {
        '+': operator.__add__,
        '-': operator.__sub__,
        '*': operator.__mul__,
        '/': operator.__truediv__,
        '^': operator.__pow__,
    }

    def eval(self, row, context):
        result = np.float64(self.value[0].eval(row, context))

        for oper, val in self.operator_operands(self.value[1:]):
            val = np.float64(val.eval(row, context))
            result = self.operation(oper, result, val)
            if np.isinf(result):
                return np.nan

        return result


class EvalMultOp(EvalBinaryArithOp):
    """Class to distinguish precedence of multiplication/division expressions.
    """
    pass


class EvalPlusOp(EvalBinaryArithOp):
    """Class to distinguish precedence of addition/subtraction expressions.
    """
    pass


class EvalExpOp(EvalBinaryArithOp):
    """Class to distinguish precedence of exponentiation expressions.
    """
    pass


class EvalComparisonOp(EvalTerm):
    """Class to evaluate comparison expressions.
    """

    opMap = {
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "!=": lambda a, b: a != b,
        "==": lambda a, b: a == b,
    }

    def eval(self, row, context):
        val1 = np.float64(self.value[0].eval(row, context))

        for oper, val in self.operator_operands(self.value[1:]):
            fn = EvalComparisonOp.opMap[oper]
            val2 = np.float64(val.eval(row, context))
            if not fn(val1, val2):
                break
            val1 = val2
        else:
            return True

        return False


class EvalNotOp(EvalTerm):
    """Class to evaluate not expressions."""

    def __init__(self, tokens):
        self.value = tokens[0][1]

    def eval(self, row, context):
        return not self.value.eval(row, context)


class EvalBinaryBooleanOp(EvalTerm):
    """Class for evaluating binary boolean operations."""

    operations = {
        'and': lambda p, q: p and q,
        'or': lambda p, q: p or q,
    }

    def eval(self, row, context):
        result = np.bool_(self.value[0].eval(row, context))

        for oper, val in self.operator_operands(self.value[1:]):
            val = np.bool_(val.eval(row, context))
            result = self.operation(oper, result, val)

        return result


class EvalAndOp(EvalBinaryBooleanOp):
    """Class to distinguish precedence of and expressions."""
    pass


class EvalOrOp(EvalBinaryBooleanOp):
    """Class to distinguish precedence of or expressions."""
    pass


class EvalInOp(EvalTerm):
    """Class to eval in expressions."""

    def eval(self, row, context):
        val_to_test = str(self.value[0].eval(row, context))
        val_list = []

        # loop through values, skip atom literal
        for val in self.value[1:]:
            val_list.append(val.eval(row, context))

        return val_to_test in val_list


class EvalCaseOp(EvalTerm):
    """Class to eval case statements."""

    def eval(self, row, context):
        # skip 'case' literal
        for token in self.value:
            case_result = token.eval(row, context)
            if case_result:
                return case_result

        return np.nan


class EvalMapOp(EvalTerm):
    """Class to eval map statements."""

    def eval(self, row, context):
        if self.tokens[0] == 'default' or self.tokens[0].eval(row, context):
            return self.tokens[1].eval(row, context)

        return False


class EvalFunction(object):
    """Class to eval functions."""

    def __init__(self, tokens):
        self.value = tokens[0][1]


class EvalDate(EvalFunction):
    """Class to evaluate date expressions."""

    def eval(self, row, context):
        # parse date from string
        return parse_str_to_unix_time(self.value.eval(row, context))


class EvalPercentile(EvalFunction):
    """Class to evaluate percentile expressions."""

    def eval(self, row, context):
        # parse date from string
        column = context.dframe[self.value.value]
        field = self.value.field(row)
        return percentileofscore(column, field)
