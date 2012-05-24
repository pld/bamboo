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
        # float or variable (faster check)
        try:
            np.float64(self.value)
            #print "returning constant: %s" % self.value
            return self.value
        except ValueError:
            #print "returning variable '%s': %s" % (self.value, row[self.value])
            return row[self.value]


class EvalMultOp(object):
    """
    Class to evaluate addition and subtraction expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        _sum = self.value[0]._eval(row)
        #print 'iterating over: %s' % self.value[1:]
        for op, val in operator_operands(self.value[1:]):
            try:
                #print 'performing: %s %s ...' % (_sum, op)
                if op == '*':
                    _sum *= np.float64(val._eval(row))
                if op == '/':
                    _sum /= np.float64(val._eval(row))
                #print "returning prod: %s" % _sum
                if np.isinf(_sum):
                    return np.nan
            except (ValueError, ZeroDivisionError):
                #print "returning error: %s" % np.nan
                return np.nan
        #print "returning prod: %s" % np.nan
        return _sum


class EvalPlusOp(object):
    """
    Class to evaluate addition and subtraction expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        _sum = self.value[0]._eval(row)
        #print 'iterating over: %s' % self.value[1:]
        for op, val in operator_operands(self.value[1:]):
            try:
                #print 'performing: %s %s ...' % (_sum, op)
                if op == '+':
                    _sum += np.float64(val._eval(row))
                if op == '-':
                    _sum -= np.float64(val._eval(row))
                #print "returning sum: %s" % _sum
                if np.isinf(_sum):
                    return np.nan
            except (ValueError, ZeroDivisionError):
                #print "returning error: %s" % np.nan
                return np.nan
        #print "returning sum: %s" % np.nan
        return _sum


class EvalComparisonOp(object):
    "Class to evaluate comparison expressions"
    opMap = {
        "<" : lambda a,b : a < b,
        "<=" : lambda a,b : a <= b,
        ">" : lambda a,b : a > b,
        ">=" : lambda a,b : a >= b,
        "!=" : lambda a,b : a != b,
        "=" : lambda a,b : a == b,
        "LT" : lambda a,b : a < b,
        "LE" : lambda a,b : a <= b,
        "GT" : lambda a,b : a > b,
        "GE" : lambda a,b : a >= b,
        "NE" : lambda a,b : a != b,
        "EQ" : lambda a,b : a == b,
        "<>" : lambda a,b : a != b,
        }
    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        val1 = np.float64(self.value[0]._eval(row))
        for op, val in operator_operands(self.value[1:]):
            fn = EvalComparisonOp.opMap[op]
            val2 = np.float64(val._eval(row))
            print 'calculated val1: %s' % val1
            print 'calculated val2: %s' % val2
            if not fn(val1, val2):
                break
            val1 = val2
        else:
            return True
        return False


class Parser(object):

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
        expop = Literal('^')
        signop = oneOf('+ -')
        multop = oneOf('* /')
        plusop = oneOf('+ -')
#        notop = CaselessLiteral('not')
#        andop = CaselessLiteral('and')
#        orop = CaselessLiteral('or')
#        lpar = Literal('(').suppress()
#        rpar = Literal(')').suppress()

        integer = Word(nums)
        real = Combine(Word(nums) + '.' + Word(nums))
        reserved_words = MatchFirst(map(Keyword, self.reserved_words))
        variable = ~reserved_words + Word(alphanums + '_')
        operand = real | integer | variable
        operand.setParseAction(EvalConstant)

        # define compound expressions
        arith_expr = operatorPrecedence(operand, [
            #("^", 2, opAssoc.RIGHT, self._eval_expop),
            #(signop, 1, opAssoc.RIGHT),
            (multop, 2, opAssoc.LEFT, EvalMultOp),
            (plusop, 2, opAssoc.LEFT, EvalPlusOp),
        ])

        comparisonop = oneOf("< <= > >= != = <> LT GT LE GE EQ NE")
        comp_expr = operatorPrecedence(arith_expr, [
            (comparisonop, 2, opAssoc.LEFT, EvalComparisonOp),
        ])

        # top level bnf
        self.bnf = comp_expr

        return self.bnf

    def parse_formula(self, input_str):
        try:
            self.parsed_expr = self.bnf.parseString(input_str)[0]
            print 'self.parsed_expr %s' % self.parsed_expr
        except ParseException, err:
            raise ParseError('Parse Failure for string "%s": %s' % (input_str,
                        err))

        def _eval(row, parser):
            return parser.parsed_expr._eval(row)

        return _eval
