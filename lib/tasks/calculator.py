from celery.task import task

from lib.parser import parse_formula
from models.observation import Observation


@task
def calculate_column(dataset, dframe, formula, name):
    """
    For calculating new columns.
    Get necessary data given a calculation ID, execute calculation formula,
    store results in dataset the calculation refers to.
    """
    # parse formula into function and variables
    func, var_stack = parse_formula(formula)

    new_column = dframe.apply(func, axis=1, var_stack=var_stack)
    new_column.name = name
    return Observation.update(dframe.join(new_column), dataset)
