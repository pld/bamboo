import copy

from pyparsing import alphanums, nums, Word, OneOrMore, ZeroOrMore,\
        ParseException, Literal

from lib.exceptions import ParseError


class Parser(object):

    bnf = None
    expr_stack = []
    var_stack = []

    def __init__(self):
        self.bnf = self.BNF()

    def push_var(self, string, loc, toks):
        self.var_stack.append(toks[0])

    def push_expr(self, string, loc, toks):
        self.expr_stack.append(toks[0])

    def BNF(self):
        """
        addop       :: '+' | '-'
        integer     :: '0'..'9'+
        variable    :: string
        term        :: integer | variable
        expression  :: term [ addop term ]*
        """
        if self.bnf:
            return self.bnf

        plus = Literal('+')
        minus = Literal('-')
        addop = plus | minus

        integer = OneOrMore(nums)
        variable = Word(alphanums + '_').setParseAction(self.push_expr)
        term = integer | variable

        expression = term + ZeroOrMore( (addop +
                    term).setParseAction(self.push_expr) )

        return expression

    def parse_formula(self, input_str):
        self.var_stack = []
        self.expr_stack = []

        try:
            parse_output = self.bnf.parseString(input_str)
        except ParseException, err:
            raise ParseError('Parse Failure: %s' % err)

        def formula_function(row, expr_stack):
            expr_stack = copy.copy(expr_stack)
            # TODO somehow don't declare this on each call
            ops = {
                '+': lambda x, y: x + y,
                '-': lambda x, y: x - y,
            }
            term = expr_stack.pop()
            if term in '+-':
                term2 = formula_function(row, expr_stack)
                term1 = formula_function(row, expr_stack)
                return ops[term](term1, term2)
            elif term.isdigit():
                return term
            else:
                # otherwise it is a variable
                return row[term]

        return [formula_function, self.expr_stack]
