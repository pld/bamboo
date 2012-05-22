# future must be first
from __future__ import division
import copy
import numpy as np
import operator

from pyparsing import alphanums, nums, CaselessLiteral, Combine, Forward,\
         Literal, OneOrMore, Optional, ParseException, Regex, Word, ZeroOrMore

from lib.exceptions import ParseError


class Parser(object):

    bnf = None
    expr_stack = []
    binary_operations = {
        '+': operator.add,
        '-': operator.sub,
        '*': operator.mul,
        '/': operator.div,
        '=': operator.eq,
        '<': operator.lt,
        '>': operator.gt,
        '<=': operator.le,
        '>=': operator.ge,
        'and': operator.__and__,
        'or': operator.__or__,
    }
    unary_operations = {
        '^': operator.__pow__,
        'not': operator.__not__,
    }
    operations = dict(binary_operations.items() + unary_operations.items())

    def __init__(self):
        self.bnf = self.BNF()

    def _push_expr(self, string, loc, toks):
        self.expr_stack.append(toks[0])

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
        notop = CaselessLiteral('not')
        andop = CaselessLiteral('and')
        orop = CaselessLiteral('or')
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
        disj = Forward()
        atom = ((real | variable).setParseAction(self._push_expr) | \
                    (lpar + disj.suppress() + rpar))
        factor = Forward()
        factor << atom + ZeroOrMore((expop +
                    factor).setParseAction(self._push_expr))
        term = factor + ZeroOrMore((multop +
                    factor).setParseAction(self._push_expr))
        expr = term + ZeroOrMore((addop +
                    term).setParseAction(self._push_expr))
        equation = expr + ZeroOrMore((compop +
                    expr).setParseAction(self._push_expr))
        neg = ZeroOrMore(notop.setParseAction(self._push_expr)) + equation
        conj = neg + ZeroOrMore((andop +
                    neg).setParseAction(self._push_expr))
        disj << conj + ZeroOrMore((orop +
                    conj).setParseAction(self._push_expr))

        # top level bnf
        self.bnf = disj

        return self.bnf

    def parse_formula(self, input_str):
        """
        Parse formula, create expression stack and generate function to evaluate
        rows. Returns the function.
        """
        self.expr_stack = []

        try:
            parse_output = self.bnf.parseString(input_str)
        except ParseException, err:
            raise ParseError('Parse Failure for string "%s": %s' % (input_str,
                        err))

        def formula_function(row, parser):
            """
            Row-wise function to calculate formula result.
            """
            if not len(parser.expr_stack):
                parser.expr_stack = copy.copy(parser.original_expr_stack)

            term = parser.expr_stack.pop()

            # operations
            if term in parser.operations.keys():
                op = parser.operations[term]
                args = [formula_function(row, parser)]

                # if binary operation get the second argument
                if term in parser.binary_operations.keys():
                    args.append(formula_function(row, parser))
                    args.reverse()

                try:
                    result = op(*[np.float64(arg) for arg in args])
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
