# future must be first
from __future__ import division
import operator

import numpy as np
from scipy.stats import percentileofscore

from bamboo.lib.datetools import safe_parse_date_to_unix_time,\
    parse_str_to_unix_time
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.utils import parse_float


def extract_binary_children(parent):
    children = [parent.value[0]]

    for oper, val in parent.operator_operands(parent.value[1:]):
        children.append(val)

    return children


class EvalTerm(object):
    """Base class for evaluation."""

    def __init__(self, tokens):
        self.tokens = tokens
        self.value = tokens[0]

    def operator_operands(self, tokenlist):
        """Generator to extract operators and operands in pairs."""
        it = iter(tokenlist)

        while 1:
            try:
                yield (it.next(), it.next())
            except StopIteration:
                break

    def operation(self, oper, result, val):
        return self.operations[oper](result, val)

    def get_children(self):
        return []

    def dependent_columns(self, context):
        return []


class EvalConstant(EvalTerm):
    """Class to evaluate a parsed constant or variable."""

    def eval(self, row, context):
        value = parse_float(self.value)
        if value is not None:
            return value

        # it may be a variable
        field = self.field(row)

        # test is date and parse as date
        return self.__parse_field(field, context)

    def __parse_field(self, field, context):
            if context.dataset and context.dataset.schema.is_date_simpletype(
                    self.value):
                field = safe_parse_date_to_unix_time(field)

            return field

    def field(self, row):
        return row.get(self.value)

    def dependent_columns(self, context):
        value = parse_float(self.value)
        if value is not None:
            return []

        # if value is not number or date, add as a column
        return [self.value]


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

    def get_children(self):
        return [self.value]


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

    def get_children(self):
        return extract_binary_children(self)


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
    """Class to evaluate comparison expressions."""

    op_map = {
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
            fn = EvalComparisonOp.op_map[oper]
            val2 = np.float64(val.eval(row, context))
            if not fn(val1, val2):
                break
            val1 = val2
        else:
            return True

        return False

    def get_children(self):
        return extract_binary_children(self)


class EvalNotOp(EvalTerm):
    """Class to evaluate not expressions."""

    def __init__(self, tokens):
        self.value = tokens[0][1]

    def eval(self, row, context):
        return not self.value.eval(row, context)

    def get_children(self):
        return [self.value]


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

    def get_children(self):
        return extract_binary_children(self)


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

        for val in self.value[1:]:
            val_list.append(val.eval(row, context))

        return val_to_test in val_list

    def get_children(self):
        return self.value


class EvalCaseOp(EvalTerm):
    """Class to eval case statements."""

    def eval(self, row, context):
        for token in self.value:
            case_result = token.eval(row, context)
            if case_result:
                return case_result

        return np.nan

    def get_children(self):
        return self.value


class EvalMapOp(EvalTerm):
    """Class to eval map statements."""

    def eval(self, row, context):
        if self.tokens[0] == 'default' or self.tokens[0].eval(row, context):
            return self.tokens[1].eval(row, context)

        return False

    def get_children(self):
        # special "default" key returns the next token (value)
        if self.tokens[0] == 'default':
            return [self.tokens[1]]

        # otherwise, return the map key and value
        return self.tokens[0:2]


class EvalFunction(object):
    """Class to eval functions."""

    def __init__(self, tokens):
        self.value = tokens[0][1]

    def get_children(self):
        return [self.value]

    def dependent_columns(self, context):
        return []


class EvalDate(EvalFunction):
    """Class to evaluate date expressions."""

    def eval(self, row, context):
        # parse date from string
        return parse_str_to_unix_time(self.value.eval(row, context))


class EvalPercentile(EvalFunction):
    """Class to evaluate percentile expressions."""

    def eval(self, row, context):
        # parse date from string
        col = self.value.value
        query_args = QueryArgs(select={col: 1})
        column = context.dataset.dframe(query_args=query_args)[col]
        field = self.value.field(row)
        return percentileofscore(column, field)

    def get_children(self):
        return []

    def dependent_columns(self, context):
        return [self.value.value]
