# future must be first
from __future__ import division
import copy
import numpy as np
import operator

from pyparsing import alphanums, nums, CaselessLiteral, Combine, Forward,\
         Keyword, Literal, MatchFirst, oneOf, OneOrMore, opAssoc, operatorPrecedence, Optional, ParseException, Regex, Word, ZeroOrMore

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
            print "returning constant: %s" % self.value
            return self.value
        except ValueError:
            print "returning variable '%s': %s" % (self.value, row[self.value])
            return row[self.value]


class EvalPlusOp(object):
    """
    Class to evaluate addition and subtraction expressions
    """

    def __init__(self, tokens):
        self.value = tokens[0]

    def _eval(self, row):
        _sum = self.value[0]._eval(row)
        for op, val in operator_operands(self.value[1:]):
            try:
                if op == '+':
                    _sum += np.float64(val._eval(row))
                if op == '-':
                    _sum -= np.float64(val._eval(row))
                print "returning sum: %s" % _sum
                return np.nan if np.isinf(_sum) else _sum
            except (ValueError, ZeroDivisionError):
                print "returning error: %s" % np.nan
                return np.nan
        print "returning sum: %s" % np.nan
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
        val1 = self.value[0]._eval(row)
        for op,val in operator_operands(self.value[1:]):
            fn = EvalComparisonOp.opMap[op]
            val2 = val._eval(row)
            print 'calculated val2: %s' % val2
            if not fn(val1,val2):
                break
            val1 = val2
        else:
            return True
        return False


class Parser(object):

    bnf = None
    reserved_words = ['and', 'or', 'not']

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
        'and': lambda x, y: x and y,
        'or': lambda x, y: x or y,
    }
    unary_operations = {
        '^': operator.__pow__,
        'not': operator.__not__,
    }
    operations = dict(binary_operations.items() + unary_operations.items())

    def __init__(self):
        self.bnf = self.BNF()

    def _push_binary(self, string, loc, toks):
        print toks
        #self.original_expr_stack.append(toks[0].toList()[1])

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
        plusop = oneOf('+ -').setDebug()
#        plus = Literal('+')
#        minus = Literal('-')
#        mult = Literal('*')
#        div = Literal('/')
#        expop = Literal('^')
#        eq = Literal('=')
#        lt = Literal('<')
#        gt = Literal('>')
#        lte = Literal('<=')
#        gte = Literal('>=')
#        notop = CaselessLiteral('not')
#        andop = CaselessLiteral('and')
#        orop = CaselessLiteral('or')
#        lpar = Literal('(').suppress()
#        rpar = Literal(')').suppress()

        # define operations
#        addop = plus | minus
#        multop = mult | div
#        compop = eq | lt | gt | lte | gte

        # define singular expressions
        #real = Regex(r'\d+(.\d+)')#.setParseAction(self._push_expr)


        integer = Word(nums)
        real = Combine(Word(nums) + '.' + Word(nums))
        reserved_words = MatchFirst(map(Keyword, self.reserved_words))
        variable = ~reserved_words + Word(alphanums + '_')
        operand = integer | real | variable
        operand.setParseAction(EvalConstant)

#        real = Regex(r'\d+(.\d+)')
#        reserved_words = MatchFirst(map(Keyword, self.reserved_words))
#        variable = ~reserved_words + Word(alphanums + '_')

        # define compound expressions
        arith_expr = operatorPrecedence(operand, [
            #("^", 2, opAssoc.RIGHT, self._eval_expop),
            #(signop, 1, opAssoc.RIGHT),
            #(multop, 2, opAssoc.LEFT, self._push_binary),
            (plusop, 2, opAssoc.LEFT, EvalPlusOp),
        ])

        comparisonop = oneOf("< <= > >= != = <> LT GT LE GE EQ NE")
        comp_expr = operatorPrecedence(arith_expr, [
            (comparisonop, 2, opAssoc.LEFT, EvalComparisonOp),
        ])

#        disj = Forward()
#        atom = (real | variable).setParseAction(self._push_expr) | \
#                    (lpar + disj.suppress() + rpar)
#        factor = Forward()
#        factor << atom + ZeroOrMore((expop +
#                    factor).setParseAction(self._push_expr))
#        term = factor + ZeroOrMore((multop +
#                    factor).setParseAction(self._push_expr))
#        expr = term + ZeroOrMore((addop +
#                    term).setParseAction(self._push_expr))
#        equation = expr + ZeroOrMore((compop +
#                    expr).setParseAction(self._push_expr))
#        #neg = (ZeroOrMore(notop) + equation).setParseAction(
#        #            self._push_expr)
#        neg = ZeroOrMore(notop).setParseAction(self._push_expr) + equation
#        conj = neg + ZeroOrMore((andop +
#                    neg).setParseAction(self._push_expr))
#        disj << conj + ZeroOrMore((orop +
#                    conj).setParseAction(self._push_expr))

        # top level bnf
        #self.bnf = disj

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

    def ___parse_formula(self, input_str):
        """
        Parse formula, create expression stack and generate function to evaluate
        rows. Returns the function.
        """
        self.expr_stack = []
        self.original_expr_stack = []

        try:
            parse_output = self.bnf.parseString(input_str)
            print "orignal_expr_stack: %s" % self.original_expr_stack
        except ParseException, err:
            raise ParseError('Parse Failure for string "%s": %s' % (input_str,
                        err))

        def formula_function(row, parser):
            """
            Row-wise function to calculate formula result.
            """
            if not len(parser.expr_stack):
                parser.expr_stack = copy.copy(parser.original_expr_stack)
            print "----"
            print "row: %s" % hash(str(row))
            print "expr_stack: %s" % self.expr_stack

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
                    print "returning: %s" % (np.nan if np.isinf(result) else result)
                    return np.nan if np.isinf(result) else result
                except (ValueError, ZeroDivisionError):
                    print "returning: %s" % np.nan
                    return np.nan

            # float or variable (faster check)
            try:
                np.float64(term)
                print "returning: %s" % term
                return term
            except ValueError:
                print "returning: %s" % row[term]
                return row[term]

        return formula_function
