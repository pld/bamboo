from bamboo.core.parser import Parser
from bamboo.lib.mongo import MONGO_ID, MONGO_ID_ENCODED
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.schema_builder import make_unique


def parse_columns(dataset, formula, name, dframe=None, no_index=False):
    """Parse a formula and return columns resulting from its functions.

    Parse a formula into a list of functions then apply those functions to
    the Data Frame and return the resulting columns.

    :param formula: The formula to parse.
    :param name: Name of the formula.
    :param dframe: A DataFrame to apply functions to.
    :param no_index: Drop the index on result columns.
    """
    functions = Parser.parse_functions(formula)
    dependent_columns = Parser.dependent_columns(formula, dataset)

    # make select from dependent_columns
    if dframe is None:
        select = {col: 1 for col in dependent_columns or [MONGO_ID]}

        dframe = dataset.dframe(
            query_args=QueryArgs(select=select),
            keep_mongo_keys=True).set_index(MONGO_ID_ENCODED)

        if not dependent_columns:
            # constant column, use dummy
            dframe['dummy'] = 0

    return __build_columns(dataset, dframe, functions, name, no_index)


def __build_columns(dataset, dframe, functions, name, no_index):
    columns = []

    for function in functions:
        column = dframe.apply(function, axis=1, args=(dataset,))
        column.name = make_unique(name, [c.name for c in columns])

        if no_index:
            column = column.reset_index(drop=True)

        columns.append(column)

    return columns
