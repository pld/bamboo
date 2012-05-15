from celery.task import task

from models.observation import Observation


@task
def calculate_column(dataset, dframe, formula, name):
    """
    For calculating new columns.
    Get necessary data given a calculation ID, execute calculation formula,
    store results in dataset the calculation refers to.
    """
    func = lambda x: formula

    new_column = dframe.apply(func, axis=1)
    new_column.name = name
    return Observation.update(dframe.join(new_column), dataset)
