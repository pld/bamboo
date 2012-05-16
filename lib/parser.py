from pyparsing import alphanums, Word, OneOrMore, ParseException

from lib.exceptions import ParseError

# grammar to parse reference to a column name
# e.g. for good eats data the formula may be: 'rating'

variable = Word(alphanums)
var_stack = []


def assign_var(_str, loc, toks):
    var_stack.append(toks[0])


pattern = OneOrMore(variable).setParseAction(assign_var)


def parse_formula(input_str):
    global var_stack
    var_stack = []

    try:
        parse_output = pattern.parseString(input_str)
    except ParseException, err:
        raise ParseError('Parse Failure')

    def formula_function(row, var_stack=var_stack):
        result = row[var_stack[0]]
        return result

    return [formula_function, var_stack]
