import copy

from pyparsing import alphanums, nums, Combine, Literal, OneOrMore, Optional,\
         ParseException, Regex, Word, ZeroOrMore

from lib.exceptions import ParseError


class Parser(object):

    bnf = None
    expr_stack = []
    var_stack = []

    def __init__(self):
        self.bnf = self.BNF()

    def _push_expr(self, string, loc, toks):
        self.expr_stack.append(toks[0])

    def BNF(self):
        """
        addop       :: '+' | '-'
        real        :: \d+(.\d+)
        variable    :: string
        term        :: real | variable
        expression  :: term [ addop term ]*
        """
        if self.bnf:
            return self.bnf

        plus = Literal('+')
        minus = Literal('-')
        point = Literal('.')
        addop = plus | minus

        real = Regex(r'\d+(.\d+)').setParseAction(self._push_expr)
        variable = Word(alphanums + '_').setParseAction(self._push_expr)
        term = real | variable

        self.bnf  = term + ZeroOrMore( (addop +
                    term).setParseAction(self._push_expr) )

        return self.bnf

    def parse_formula(self, input_str):
        self.var_stack = []
        self.expr_stack = []

        try:
            parse_output = self.bnf.parseString(input_str)
        except ParseException, err:
            raise ParseError('Parse Failure: %s' % err)

        def formula_function(row, expr_stack):
            expr_stack = copy.copy(expr_stack)
            # TODO don't declare this on each call
            ops = {
                '+': lambda x, y: float(x) + float(y),
                '-': lambda x, y: float(x) - float(y),
            }
            term = expr_stack.pop()

            if term in '+-':
                term2 = formula_function(row, expr_stack)
                expr_stack.pop()
                term1 = formula_function(row, expr_stack)
                expr_stack.pop()
                try:
                    return ops[term](term1, term2)
                except ValueError:
                    return 'null'

            # faster check for float
            is_float = False
            try:
                float(term)
                is_float = True
            except ValueError:
                pass
            if is_float:
                return term
            else:
                # otherwise it is a variable
                return row[term]

        return [formula_function, self.expr_stack]
