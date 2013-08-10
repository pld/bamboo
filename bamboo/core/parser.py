from functools import partial

from pyparsing import alphanums, nums, oneOf, opAssoc, operatorPrecedence,\
    CaselessLiteral, Combine, Keyword, Literal, MatchFirst, Optional,\
    ParseException, Regex, Word, ZeroOrMore

from bamboo.core.aggregations import AGGREGATIONS
from bamboo.core.operations import EvalAndOp, EvalCaseOp, EvalComparisonOp,\
    EvalConstant, EvalExpOp, EvalDate, EvalInOp, EvalMapOp, EvalMultOp,\
    EvalNotOp, EvalOrOp, EvalPercentile, EvalPlusOp, EvalSignOp, EvalString,\
    EvalToday


def build_caseless_or_expression(strings):
    literals = [CaselessLiteral(aggregation) for aggregation in strings]
    return reduce(lambda or_expr, literal: or_expr | literal, literals)


def get_dependent_columns(parsed_expr, dataset):
    return __find_dependent_columns(dataset, parsed_expr, [])


def __find_dependent_columns(dataset, parsed_expr, result):
    """Find dependent columns for a dataset and parsed expression.

    :param dataset: The dataset to find dependent columns for.
    :param parsed_expr: The parsed formula expression.
    """
    dependent_columns = parsed_expr.dependent_columns(dataset)
    result.extend(dependent_columns)

    for child in parsed_expr.get_children():
        __find_dependent_columns(dataset, child, result)

    return result


class ParseError(Exception):
    """For errors while parsing formulas."""
    pass


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
    function_names = ['date', 'percentile', 'today']
    operator_names = ['and', 'or', 'not', 'in']
    parsed_expr = None
    special_names = ['default']

    reserved_words = aggregation_names + function_names + operator_names +\
        special_names

    def __init__(self):
        self.bnf = self.__build_bnf()

    @classmethod
    def dependent_columns(cls, formula, dataset):
        functions, _ = cls.parse(formula)
        columns = [get_dependent_columns(f, dataset) for f in functions]

        return set.union(set(), *columns)

    @property
    def functions(self):
        return self.column_functions if self.aggregation else self.parsed_expr

    def store_aggregation(self, _, __, tokens):
        """Cached a parsed aggregation."""
        self.aggregation = tokens[0]
        self.column_functions = tokens[1:]

    def __build_bnf(self):
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

        # aggregation functions
        aggregations = build_caseless_or_expression(self.aggregation_names)

        # literal syntactic
        open_bracket = Literal('[').suppress()
        close_bracket = Literal(']').suppress()
        open_paren = Literal('(').suppress()
        close_paren = Literal(')').suppress()
        comma = Literal(',').suppress()
        dquote = Literal('"').suppress()
        colon = Literal(':').suppress()

        # functions
        date_func = CaselessLiteral('date')
        percentile_func = CaselessLiteral('percentile')
        today_func = CaselessLiteral('today()').setParseAction(EvalToday)

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
        ]) | today_func

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

        return ((aggregations + open_paren + Optional(
            trans_expr + ZeroOrMore(comma + trans_expr)))
            .setParseAction(self.store_aggregation) + close_paren)\
            | trans_expr

    @classmethod
    def parse(cls, formula):
        """Parse formula and return evaluation function.

        Parse `formula` into an aggregation name and functions.
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

        :param formula: The string to parse.

        :returns: A tuple with the name of the aggregation in the formula, if
           any and a list of functions built from the input string.
        """
        parser = cls()

        try:
            parser.parsed_expr = parser.bnf.parseString(formula, parseAll=True)
        except ParseException, err:
            raise ParseError('Parse Failure for string "%s": %s' % (
                             formula, err))

        return [parser.functions, parser.aggregation]

    @classmethod
    def parse_aggregation(cls, formula):
        _, a = cls.parse(formula)
        return a

    @classmethod
    def parse_function(cls, formula):
        return cls.parse_functions(formula)[0]

    @classmethod
    def parse_functions(cls, formula):
        return [partial(f.eval) for f in cls.parse(formula)[0]]

    @classmethod
    def validate(cls, dataset, formula, groups):
        """Validate `formula` and `groups` for dataset.

        Validate the formula and group string by attempting to get a row from
        the dframe for the dataset and then running parser validation on this
        row. Additionally, ensure that the groups in the group string are
        columns in the dataset.

        :param dataset: The dataset to validate for.
        :param formula: The formula to validate.
        :param groups: A list of columns to group by.

        :returns: The aggregation (or None) for the formula.
        """
        cls.validate_formula(formula, dataset)

        for group in groups:
            if not group in dataset.schema.keys():
                raise ParseError(
                    'Group %s not in dataset columns.' % group)

    @classmethod
    def validate_formula(cls, formula, dataset):
        """Validate the *formula* on an example *row* of data.

        Rebuild the BNF then parse the `formula` given the sample `row`.

        :param formula: The formula to validate.
        :param dataset: The dataset to validate against.

        :returns: The aggregation for the formula.
        """
        # check valid formula
        cls.parse(formula)
        schema = dataset.schema

        if not schema:
            raise ParseError(
                'No schema for dataset, please add data or wait for it to '
                'finish processing')

        for column in cls.dependent_columns(formula, dataset):
            if column not in schema.keys():
                raise ParseError('Missing column reference: %s' % column)

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
        ]

    def __setstate__(self, state):
        """Set internal variables from pickled state."""
        self.aggregation, self.aggregation_names, self.function_names,\
            self.operator_names, self.special_names, self.reserved_words,\
            self.special_names = state
        self.__build_bnf()
