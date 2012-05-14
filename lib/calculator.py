from celery.task import task

from models.observation import Observation


class Calculator(object):
    """
    For calculating new columns.
    """

    @task
    def run(dataset, dframe, formula, name):
        """
        Get necessary data given a calculation ID, execute calculation formula,
        store results in dataset the calculation refers to.
        """
        func = lambda x: formula

        new_column = dframe.apply(func, axis=1)
        new_column.name = name
        return Observation.update(dframe.join(new_column), dataset)
