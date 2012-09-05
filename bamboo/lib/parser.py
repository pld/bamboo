from pyparsing import alphanums, nums, oneOf, opAssoc, operatorPrecedence,\
    CaselessLiteral, Combine, Forward, Keyword, Literal, MatchFirst,\
    OneOrMore, Optional, ParseException, Regex, Word, ZeroOrMore

from lib.constants import SCHEMA
from lib.exceptions import ParseError
from lib.operations import EvalAndOp, EvalComparisonOp, EvalConstant,\
    EvalExpOp, EvalDate, EvalInOp, EvalMultOp, EvalNotOp, EvalOrOp,\
    EvalPlusOp, EvalSignOp, EvalString


class ParserContext(object):
    """
    Context to be passed into parser.
    """

    def __init__(self, dataset):
        self.schema = dataset.get(SCHEMA)


class Parser(object):
    """
    Class for parsing and evaluating formula.
    """

    aggregation = None
    bnf = None
    aggregation_names = ['sum']
    function_names = ['date', 'years']
    operator_names = ['and', 'or', 'not', 'in']
    reserved_words = aggregation_names + function_names + operator_names

    def __init__(self, dataset=None):
        self.context = ParserContext(dataset) if dataset else None
        self.bnf = self.BNF()

    def set_aggregation(self, string, location, tokens):
        self.aggregation = tokens[0]

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
        integer     \d+
        variable    \w+
        string      ".+"
        atom        real | integer | variable
        func        func ( atom )
        factor      atom [ expop factor]*
        term        factor [ multop factor ]*
        expr        term [ addop term ]*
        equation    expr [compop expr]*
        in          string in '[' "string"[, "string"]* ']'
        neg         [notop]* equation | in
        conj        neg [andop neg]*
        disj        conj [orop conj]*
        agg         agg ( disj )
        =========   ==========

        Examples:

        - constants
            - 9 + 5',
            - aliases
            - rating',
            - gps',
        - arithmetic
            - amount + gps_alt',
            - amount - gps_alt',
            - amount + 5',
            - amount - gps_alt + 2.5',
            - amount * gps_alt',
            - amount / gps_alt',
            - amount * gps_alt / 2.5',
            - amount + gps_alt * gps_precision',
        - precedence
            - amount + gps_alt) * gps_precision',
        - comparison
            - amount = 2',
            - 10 < amount',
            - 10 < amount + gps_alt',
        - logical
            - not amount = 2',
            - not(amount = 2)',
            - amount = 2 and 10 < amount',
            - amount = 2 or 10 < amount',
            - not not amount = 2 or 10 < amount',
            - not amount = 2 or 10 < amount',
            - not amount = 2) or 10 < amount',
            - not(amount = 2 or 10 < amount)',
            - amount ^ 3',
            - amount + gps_alt) ^ 2 + 100',
            - amount',
            - amount < gps_alt - 100',
            - membership
            - rating in ["delectible"]',
            - risk_factor in ["low_risk"]',
            - amount in ["9.0", "2.0", "20.0"]',
            - risk_factor in ["low_risk"]) and (amount in ["9.0", "20.0"])',
        - dates
            - date("09-04-2012") - submit_date > 21078000',

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

        # functions
        date_func = CaselessLiteral('date')

        # aggregation functions
        sum_agg = CaselessLiteral('sum').setParseAction(self.set_aggregation)

        # literal syntactic
        open_bracket = Literal('[').suppress()
        close_bracket = Literal(']').suppress()
        open_paren = Literal('(').suppress()
        close_paren = Literal(')').suppress()
        comma = Literal(',')
        dquote = Literal('"').suppress()

        reserved_words = MatchFirst(map(Keyword, self.reserved_words))

        # atoms
        integer = Word(nums)
        real = Combine(Word(nums) + '.' + Word(nums))
        variable = ~reserved_words + Word(alphanums + '_')
        atom = real | integer | variable
        atom.setParseAction(EvalConstant)

        # everything between pairs of double quotes is a string
        string = dquote + Regex('[^"]+') + dquote
        string.setParseAction(EvalString)

        # expressions
        in_list = open_bracket + string +\
            ZeroOrMore(comma + string) + close_bracket

        func_expr = operatorPrecedence(string, [
            (date_func, 1, opAssoc.RIGHT, EvalDate),
        ])

        arith_expr = operatorPrecedence(atom | func_expr, [
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

        agg_expr = (sum_agg.suppress() + open_paren + prop_expr + close_paren
                    ) | prop_expr

        # top level bnf
        self.bnf = agg_expr

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

        def function(row, parser):
            return parser.parsed_expr._eval(row, parser.context)

        return self.aggregation, function

    def validate_formula(self, formula, row):
        """
        Validate the *formula* on an example *row* of data.  Rebuild the BNF
        """
        # remove saved aggregation
        self.aggregation = None

        # check valid formula
        aggregation, function = self.parse_formula(formula)
        try:
            function(row, self)
        except KeyError, err:
            raise ParseError('Missing column reference: %s' % err)
