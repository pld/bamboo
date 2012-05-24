from celery.task import task

from lib.parser import Parser
from models.observation import Observation


@task
def calculate_column(dataset, dframe, formula, name):
    """
    For calculating new columns.
    Get necessary data given a calculation ID, execute calculation formula,
    store results in dataset the calculation refers to.
    """
    # parse formula into function and variables
    parser = Parser()
    func = parser.parse_formula(formula)

    new_column = dframe.apply(func, axis=1, args=(parser, ))
    new_column.name = name
    #print new_column
    return Observation.update(dframe.join(new_column), dataset)
