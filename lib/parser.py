from pyparsing import alphanums, nums, oneOf, opAssoc, operatorPrecedence,\
         CaselessLiteral, Combine, Forward, Keyword, Literal, MatchFirst,\
         OneOrMore, Optional, ParseException, Regex, Word, ZeroOrMore

from lib.exceptions import ParseError
from lib.operations import EvalAndOp, EvalComparisonOp, EvalConstant,\
         EvalExpOp, EvalInOp, EvalMultOp, EvalNotOp, EvalOrOp, EvalPlusOp,\
         EvalSignOp, EvalString


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

        =========   ==========
        Operation   Expression
        =========   ==========
        addop       '+' | '-'
        multop      '*' | '/'
        expop       '^'
        compop      '=' | '<' | '>' | '<=' | '>='
        notop       'not'
        andop       'and'
        orop        'or'
        real        \d+(.\d+)
        variable    string
        in          variable in '[' "variable"[, "variable"]* ']'
        atom        real | variable | [notop]* '(' disj ')'
        factor      atom [ expop factor]*
        term        factor [ multop factor ]*
        expr        term [ addop term ]*
        equation    expr [compop expr]*
        neg         [notop]* equation
        conj        neg [andop neg]*
        disj        conj [orop conj]*
        =========   ==========
        """
        if self.bnf:
            return self.bnf

        # literal operators
        exp_op = Literal('^')
        sign_op = oneOf('+ -')
        mult_op = oneOf('* /')
        plus_op = oneOf('+ -')
        not_op = CaselessLiteral('not')
        and_op = CaselessLiteral('and')
        or_op = CaselessLiteral('or')
        in_op = CaselessLiteral('in')
        comparison_op = oneOf('< <= > >= != =')

        # literal syntactic
        open_bracket = Literal('[').suppress()
        close_bracket = Literal(']').suppress()
        comma = Literal(',')
        dquote = Literal('"').suppress()

        # operands
        integer = Word(nums)
        real = Combine(Word(nums) + '.' + Word(nums))
        reserved_words = MatchFirst(map(Keyword, self.reserved_words))
        variable = ~reserved_words + Word(alphanums + '_')
        string = (dquote + (real | integer | variable) + dquote).setParseAction(
                EvalString)
        operand = real | integer | variable
        operand.setParseAction(EvalConstant)

        # expressions
        in_list = open_bracket + string +\
                ZeroOrMore(comma + string) + close_bracket

        arith_expr = operatorPrecedence(operand, [
            (sign_op, 1, opAssoc.RIGHT, EvalSignOp),
            (exp_op, 2, opAssoc.RIGHT, EvalExpOp),
            (mult_op, 2, opAssoc.LEFT, EvalMultOp),
            (plus_op, 2, opAssoc.LEFT, EvalPlusOp),
        ])

        comp_expr = operatorPrecedence(arith_expr, [
            (comparison_op, 2, opAssoc.LEFT, EvalComparisonOp),
        ])

        prop_expr = operatorPrecedence(comp_expr | in_list, [
            (in_op, 2, opAssoc.RIGHT, EvalInOp),
            (not_op, 1, opAssoc.RIGHT, EvalNotOp),
            (and_op, 2, opAssoc.LEFT, EvalAndOp),
            (or_op, 2, opAssoc.LEFT, EvalOrOp),
        ])

        # top level bnf
        self.bnf = prop_expr

        return self.bnf

    def parse_formula(self, input_str):
        """
        Parse formula and return evaluation function.
        """

        try:
            self.parsed_expr = self.bnf.parseString(input_str)[0]
        except ParseException, err:
            raise ParseError('Parse Failure for string "%s": %s' % (input_str,
                        err))

        def _eval(row, parser):
            return parser.parsed_expr._eval(row)

        return _eval

    def validate_formula(self, formula, row):
        # check valid formula
        _eval = self.parse_formula(formula)
        try:
            _eval(row, self)
        except KeyError, err:
            raise ParseError('Missing column "%s": %s' % (1, err))
