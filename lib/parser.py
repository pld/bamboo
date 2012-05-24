# future must be first
from __future__ import division
import copy
import numpy as np
import operator

from pyparsing import alphanums, nums, oneOf, opAssoc, operatorPrecedence,\
         CaselessLiteral, Combine, Forward, Keyword, Literal, MatchFirst,\
         OneOrMore, Optional, ParseException, Regex, Word, ZeroOrMore

from lib.exceptions import ParseError


def operator_operands(tokenlist):
    "generator to extract operators and operands in pairs"
    it = iter(tokenlist)
    while 1:
        try:
            yield (it.next(), it.next())
        except StopIteration:
            break


class EvalConstant(object):
    """
    Class to evaluate a parsed constant or variable
    """
    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        try:
            np.float64(self.value)
            return self.value
        except ValueError:
            return row[self.value]


class EvalSignOp(object):
    """
    Class to evaluate expressions with a leading + or - sign
    """

    def __init__(self, tokens):
        self.sign, self.value = tokens[0]

    def _eval(self, row):
        mult = {'+':1, '-':-1}[self.sign]
        return mult * self.value._eval(row)


class EvalExpOp(object):
    """
    Class to evaluate exponentiation expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        _exp = self.value[0]._eval(row)
        for op, val in operator_operands(self.value[1:]):
            try:
                #print 'performing: %s %s ...' % (_sum, op)
                _exp **= np.float64(val._eval(row))
                #print "returning prod: %s" % _sum
                if np.isinf(_exp):
                    return np.nan
            except (ValueError, ZeroDivisionError):
                #print "returning error: %s" % np.nan
                return np.nan
        #print "returning prod: %s" % np.nan
        return _exp


class EvalMultOp(object):
    """
    Class to evaluate addition and subtraction expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        try:
            _mult = np.float64(self.value[0]._eval(row))
            for op, val in operator_operands(self.value[1:]):
                    if op == '*':
                        _mult *= np.float64(val._eval(row))
                    if op == '/':
                        _mult /= np.float64(val._eval(row))
                    if np.isinf(_mult):
                        return np.nan
        except (ValueError, ZeroDivisionError):
            return np.nan
        return _mult


class EvalPlusOp(object):
    """
    Class to evaluate addition and subtraction expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        try:
            _sum = np.float64(self.value[0]._eval(row))
            for op, val in operator_operands(self.value[1:]):
                    if op == '+':
                        _sum += np.float64(val._eval(row))
                    if op == '-':
                        _sum -= np.float64(val._eval(row))
                    if np.isinf(_sum):
                        return np.nan
        except (ValueError, ZeroDivisionError):
            return np.nan
        return _sum


class EvalComparisonOp(object):
    """
    Class to evaluate comparison expressions
    """

    opMap = {
        "<" : lambda a,b : a < b,
        "<=" : lambda a,b : a <= b,
        ">" : lambda a,b : a > b,
        ">=" : lambda a,b : a >= b,
        "!=" : lambda a,b : a != b,
        "=" : lambda a,b : a == b,
    }

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        val1 = np.float64(self.value[0]._eval(row))
        for op, val in operator_operands(self.value[1:]):
            fn = EvalComparisonOp.opMap[op]
            val2 = np.float64(val._eval(row))
            if not fn(val1, val2):
                break
            val1 = val2
        else:
            return True
        return False


class EvalNotOp(object):
    """
    Class to evaluate not expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0][1]

    def _eval(self, row):
        return not self.value._eval(row)


class EvalAndOp(object):
    """
    Class to evaluate and expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        _conj = np.bool_(self.value[0]._eval(row))
        for op, val in operator_operands(self.value[1:]):
            _conj = _conj and np.bool_(val._eval(row))
        return _conj


class EvalOrOp(object):
    """
    Class to evaluate or expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        _disj = np.bool_(self.value[0]._eval(row))
        for op, val in operator_operands(self.value[1:]):
            _disj = _disj or np.bool_(val._eval(row))
        return _disj


class Parser(object):
    """
    Class for parsing and evaluating formula.
    """

    bnf = None
    reserved_words = ['and', 'or', 'not']

    def __init__(self):
        self.bnf = self.BNF()

    def BNF(self):
        """
        Backus-Naur Form of formula language:

        addop       :: '+' | '-'
        multop      :: '*' | '/'
        expop       :: '^'
        compop      :: '=' | '<' | '>' | '<=' | '>='
        notop       :: 'not'
        andop       :: 'and'
        orop        :: 'or'
        real        :: \d+(.\d+)
        variable    :: string
        atom        :: real | variable | '(' disj ')'
        factor      :: atom [ expop factor]*
        term        :: factor [ multop factor ]*
        expr        :: term [ addop term ]*
        equation    :: expr [compop expr]*
        neg         :: [notop]* equation
        conj        :: neg [andop neg]*
        disj        :: conj [orop conj]*
        """
        if self.bnf:
            return self.bnf

        # define literals
        exp_op = Literal('^')
        sign_op = oneOf('+ -')
        mult_op = oneOf('* /')
        plus_op = oneOf('+ -')
        not_op = CaselessLiteral('not')
        and_op = CaselessLiteral('and')
        or_op = CaselessLiteral('or')

        integer = Word(nums)
        real = Combine(Word(nums) + '.' + Word(nums))
        reserved_words = MatchFirst(map(Keyword, self.reserved_words))
        variable = ~reserved_words + Word(alphanums + '_')
        operand = real | integer | variable
        operand.setParseAction(EvalConstant)

        # define compound expressions
        arith_expr = operatorPrecedence(operand, [
            (exp_op, 2, opAssoc.RIGHT, EvalExpOp),
            (sign_op, 1, opAssoc.RIGHT, EvalSignOp),
            (mult_op, 2, opAssoc.LEFT, EvalMultOp),
            (plus_op, 2, opAssoc.LEFT, EvalPlusOp),
        ])

        comparison_op = oneOf("< <= > >= != =")
        comp_expr = operatorPrecedence(arith_expr, [
            (comparison_op, 2, opAssoc.LEFT, EvalComparisonOp),
        ])

        prop_expr = operatorPrecedence(comp_expr, [
            (not_op, 1, opAssoc.RIGHT, EvalNotOp),
            (and_op, 2, opAssoc.LEFT, EvalAndOp),
            (or_op, 2, opAssoc.LEFT, EvalOrOp),
        ])

        # top level bnf
        self.bnf = prop_expr

        return self.bnf

    def parse_formula(self, input_str):
        try:
            self.parsed_expr = self.bnf.parseString(input_str)[0]
        except ParseException, err:
            raise ParseError('Parse Failure for string "%s": %s' % (input_str,
                        err))

        def _eval(row, parser):
            return parser.parsed_expr._eval(row)

        return _eval
