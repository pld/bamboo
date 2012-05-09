import threading

import pandas

from models.observation import Observation


class Calculator(threading.Thread):
    """
    Thread for calculating new columns.
    """

    def __init__(self, calculation, dataset, formula, name):
        self._calculation = calculation
        self._dataset = dataset
        self._formula = formula
        self._name = name
        threading.Thread.__init__(self)

    def run(self):
        dframe = Observation.find(self._dataset, as_df=True)
        func = lambda x: self._calculation[self._formula]
        new_column = dframe.apply(func, axis=1)
        new_column.name = self._calculation[self._name]
        dframe.join(new_column)
        Observation.update(dframe, self._dataset)
