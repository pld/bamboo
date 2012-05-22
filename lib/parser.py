# future must be first
from __future__ import division
import copy
import numpy as np
import operator

from pyparsing import alphanums, nums, Combine, Forward, Literal, OneOrMore,\
         Optional, ParseException, Regex, Word, ZeroOrMore

from lib.exceptions import ParseError


class Parser(object):

    bnf = None
    expr_stack = []
    var_stack = []
    operations = {
        '+': operator.add,
        '-': operator.sub,
        '*': operator.mul,
        '/': operator.div,
        '^': operator.__pow__,
        '=': operator.eq,
        '<': operator.lt,
        '>': operator.gt,
        '<=': operator.le,
        '>=': operator.ge,
    }

    def __init__(self):
        self.bnf = self.BNF()

    def _push_expr(self, string, loc, toks):
        self.expr_stack.append(toks[0])

    def BNF(self):
        """
        addop       :: '+' | '-'
        multop      :: '*' | '/'
        expop       :: '^'
        compop      :: '=' | '<' | '>' | '<=' | '>='
        real        :: \d+(.\d+)
        variable    :: string
        atom        :: real | variable | '(' equation ')'
        factor      :: atom [ expop factor]*
        term        :: factor [ multop factor ]*
        expr        :: term [ addop term ]*
        equation    :: expr [compop expr]*
        """
        if self.bnf:
            return self.bnf

        # define literals
        plus = Literal('+')
        minus = Literal('-')
        mult = Literal('*')
        div = Literal('/')
        expop = Literal('^')
        eq = Literal('=')
        lt = Literal('<')
        gt = Literal('>')
        lte = Literal('<=')
        gte = Literal('>=')
        lpar = Literal('(').suppress()
        rpar = Literal(')').suppress()

        # define operations
        addop = plus | minus
        multop = mult | div
        compop = eq | lt | gt | lte | gte

        # define singular expressions
        real = Regex(r'\d+(.\d+)')
        variable = Word(alphanums + '_')

        # define compound expressions
        equation = Forward()
        atom = ((real | variable).setParseAction(self._push_expr) | \
                    (lpar + equation.suppress() + rpar))
        factor = Forward()
        factor << atom + ZeroOrMore((expop +
                    factor).setParseAction(self._push_expr))
        term = factor + ZeroOrMore((multop +
                    factor).setParseAction(self._push_expr))
        expr = term + ZeroOrMore((addop +
                    term).setParseAction(self._push_expr))
        equation << expr + ZeroOrMore((compop +
                    expr).setParseAction(self._push_expr))

        # top level bnf
        self.bnf = equation

        return self.bnf

    def parse_formula(self, input_str):
        self.var_stack = []
        self.expr_stack = []

        try:
            parse_output = self.bnf.parseString(input_str)
        except ParseException, err:
            raise ParseError('Parse Failure: %s' % err)

        def formula_function(row, parser):
            """
            Row-wise function to calculate formula result.
            """
            ops = parser.operations

            if not len(parser.expr_stack):
                parser.expr_stack = copy.copy(parser.original_expr_stack)

            term = parser.expr_stack.pop()

            # operation
            if term in ops.keys():
                term2 = formula_function(row, parser)
                term1 = formula_function(row, parser)
                try:
                    result = ops[term](np.float64(term1), np.float64(term2))
                    return np.nan if np.isinf(result) else result
                except (ValueError, ZeroDivisionError):
                    return np.nan

            # float or variable (faster check)
            try:
                np.float64(term)
                return term
            except ValueError:
                return row[term]

        self.original_expr_stack = copy.copy(self.expr_stack)
        return formula_function
