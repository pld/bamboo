from functools import partial

from pyparsing import alphanums, nums, oneOf, opAssoc, operatorPrecedence,\
    CaselessLiteral, Combine, Keyword, Literal, MatchFirst, Optional,\
    ParseException, Regex, Word, ZeroOrMore

from bamboo.core.aggregations import AGGREGATIONS
from bamboo.core.operations import EvalAndOp, EvalCaseOp, EvalComparisonOp,\
    EvalConstant, EvalExpOp, EvalDate, EvalInOp, EvalMapOp, EvalMultOp,\
    EvalNotOp, EvalOrOp, EvalPercentile, EvalPlusOp, EvalSignOp, EvalString


class ParseError(Exception):
    """For errors while parsing formulas."""
    pass


class ParserContext(object):
    """Context to be passed into parser."""

    dependent_columns = set()
    schema = None

    def __init__(self, dataset=None):
        if dataset:
            self.dframe = dataset.dframe()
            self.schema = dataset.schema


class Parser(object):
    """Class for parsing and evaluating formula.

    Attributes:

    - aggregation: Aggregation parsed from formula.
    - aggregation_names: Possible aggregations.
    - bnf: Cached Backus-Naur Form of formula.
    - column_functions: Cached additional columns as aggregation parameters.
    - function_names: Names of possible functions in formulas.
    - operator_names: Names of possible operators in formulas.
    - parsed_expr: Cached parsed expression.
    - special_names: Names of possible reserved names in formulas.
    - reserved_words: List of all possible reserved words that may be used in
      formulas.
    """

    aggregation = None
    aggregation_names = AGGREGATIONS.keys()
    bnf = None
    column_functions = None
    function_names = ['date', 'percentile', 'years']
    operator_names = ['and', 'or', 'not', 'in']
    parsed_expr = None
    special_names = ['default']

    reserved_words = aggregation_names + function_names + operator_names +\
        special_names

    def __init__(self, dataset=None):
        """Create parser and set context."""
        self.context = ParserContext(dataset)
        self._build_bnf()

    def store_aggregation(self, _, __, tokens):
        """Cached a parsed aggregation."""
        self.aggregation = tokens[0]
        self.column_functions = tokens[1:]

    def _build_bnf(self):
        """Parse formula to function based on language definition.

        Backus-Naur Form of formula language:

        =========   ==========
        Operation   Expression
        =========   ==========
        addop       '+' | '-'
        multop      '*' | '/'
        expop       '^'
        compop      '==' | '<' | '>' | '<=' | '>='
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
        case        'case' disj: atom[, disj: atom]*[, 'default': atom]
        trans       trans ( case )
        agg         agg ( trans[, trans]* )
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
        in_op = CaselessLiteral('in').suppress()
        comparison_op = oneOf('< <= > >= != ==')
        case_op = CaselessLiteral('case').suppress()

        # functions
        date_func = CaselessLiteral('date')
        percentile_func = CaselessLiteral('percentile')

        # aggregation functions
        aggregations = self._build_caseless_or_expression(
            self.aggregation_names)

        # literal syntactic
        open_bracket = Literal('[').suppress()
        close_bracket = Literal(']').suppress()
        open_paren = Literal('(').suppress()
        close_paren = Literal(')').suppress()
        comma = Literal(',').suppress()
        dquote = Literal('"').suppress()
        colon = Literal(':').suppress()

        # case statment
        default = CaselessLiteral('default')

        reserved_words = MatchFirst(
            [Keyword(word) for word in self.reserved_words])

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

        default_statement = (default + colon + atom).setParseAction(EvalMapOp)
        map_statement = (prop_expr + colon + atom).setParseAction(EvalMapOp)

        case_list = map_statement + ZeroOrMore(
            comma + map_statement) + Optional(comma + default_statement)

        case_expr = operatorPrecedence(case_list, [
            (case_op, 1, opAssoc.RIGHT, EvalCaseOp),
        ]) | prop_expr

        trans_expr = operatorPrecedence(case_expr, [
            (percentile_func, 1, opAssoc.RIGHT, EvalPercentile),
        ])

        agg_expr = ((
                    aggregations + open_paren +
                    Optional(trans_expr + ZeroOrMore(comma + trans_expr)))
                    .setParseAction(self.store_aggregation) + close_paren)\
            | trans_expr

        # top level bnf
        self.bnf = agg_expr

    def parse_formula(self, input_str):
        """Parse formula and return evaluation function.

        Parse `input_str` into an aggregation name and functions.
        There will be multiple functions is the aggregation takes multiple
        arguments, e.g. ratio which takes a numerator and denominator formula.

        Examples:

        - constants
            - ``9 + 5``,
        - aliases
            - ``rating``,
            - ``gps``,
        - arithmetic
            - ``amount + gps_alt``,
            - ``amount - gps_alt``,
            - ``amount + 5``,
            - ``amount - gps_alt + 2.5``,
            - ``amount * gps_alt``,
            - ``amount / gps_alt``,
            - ``amount * gps_alt / 2.5``,
            - ``amount + gps_alt * gps_precision``,
        - precedence
            - ``(amount + gps_alt) * gps_precision``,
        - comparison
            - ``amount == 2``,
            - ``10 < amount``,
            - ``10 < amount + gps_alt``,
        - logical
            - ``not amount == 2``,
            - ``not(amount == 2)``,
            - ``amount == 2 and 10 < amount``,
            - ``amount == 2 or 10 < amount``,
            - ``not not amount == 2 or 10 < amount``,
            - ``not amount == 2 or 10 < amount``,
            - ``not amount == 2) or 10 < amount``,
            - ``not(amount == 2 or 10 < amount)``,
            - ``amount ^ 3``,
            - ``amount + gps_alt) ^ 2 + 100``,
            - ``amount``,
            - ``amount < gps_alt - 100``,
        - membership
            - ``rating in ["delectible"]``,
            - ``risk_factor in ["low_risk"]``,
            - ``amount in ["9.0", "2.0", "20.0"]``,
            - ``risk_factor in ["low_risk"]) and (amount in ["9.0", "20.0"])``,
        - dates
            - ``date("09-04-2012") - submit_date > 21078000``,
        - cases
            - ``case food_type in ["morning_food"]: 1, default: 3``
        - transformations: row-wise column based aggregations
            - ``percentile(amount)``

        :param dataset: The dataset to base context on, default is None.
        :param input_str: The string to parse.

        :returns: A tuple with the name of the aggregation in the formula, if
            any and a list of functions built from the input string.
        """

        # reset dependent columns before parsing
        self.context.dependent_columns = set()

        try:
            self.parsed_expr = self.bnf.parseString(
                input_str, parseAll=True)[0]
        except ParseException, err:
            raise ParseError('Parse Failure for string "%s": %s' % (input_str,
                             err))

        functions = []

        if self.aggregation:
            for column_function in self.column_functions:
                functions.append(partial(column_function.eval))
        else:
            functions.append(partial(self.parsed_expr.eval))

        return self.aggregation, functions

    def validate_formula(self, formula, row):
        """Validate the *formula* on an example *row* of data.

        Rebuild the BNF then parse the `formula` given the sample `row`.

        :param formula: The formula to validate.
        :param row: A sample row to check the formula against.

        :returns: The aggregation for the formula.
        """
        # remove saved aggregation
        self.aggregation = None

        # check valid formula
        aggregation, functions = self.parse_formula(formula)

        if not self.context.schema:
            raise ParseError(
                'No schema for dataset, please add data or wait for it to '
                'finish processing')

        try:
            for function in functions:
                function(row, self.context)
        except KeyError, err:
            raise ParseError('Missing column reference: %s' % err)

        return aggregation

    def _build_caseless_or_expression(self, strings):
        literals = [
            CaselessLiteral(aggregation) for
            aggregation in self.aggregation_names
        ]
        return reduce(lambda or_expr, literal: or_expr | literal, literals)

    def __getstate__(self):
        """Get state for pickle."""
        return [
            self.aggregation,
            self.aggregation_names,
            self.function_names,
            self.operator_names,
            self.special_names,
            self.reserved_words,
            self.special_names,
            self.context,
        ]

    def __setstate__(self, state):
        """Set internal variables from pickled state."""
        self.aggregation, self.aggregation_names, self.function_names,\
            self.operator_names, self.special_names, self.reserved_words,\
            self.special_names, self.context = state
        self._build_bnf()
