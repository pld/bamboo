from collections import defaultdict

from celery.task import task
from pandas import DataFrame

from lib.constants import DATASET_ID, LINKED_DATASETS
from models.dataset import Dataset
from models.observation import Observation


def sum_dframe(column):
    return column.sum()


@task
def calculate_column(parser, dataset, dframe, formula, name, group=None,
                     query=None):
    """
    For calculating new columns.
    Get necessary data given a calculation ID, execute calculation formula,
    store results in dataset the calculation refers to.
    """
    # parse formula into function and variables
    aggregation, function = parser.parse_formula(formula)

    new_column = dframe.apply(function, axis=1, args=(parser, ))
    new_column.name = name

    if aggregation:
        if group:
            # groupby on dframe then run aggregation on groupby obj
            new_dframe = DataFrame(dframe[group]).join(new_column).\
                groupby(group, as_index=False).agg(aggregation)
        else:
            result = {
                'sum': sum_dframe,
            }[aggregation](new_column)
            new_dframe = DataFrame({name: [result]})

        new_dataset = Dataset.create()
        Observation.save(new_dframe, new_dataset)
        linked_datasets = dataset.get(LINKED_DATASETS, defaultdict(list))
        # Mongo does not allow None as a key
        linked_datasets[group or ''].append(new_dataset[DATASET_ID])
        Dataset.update(dataset, {LINKED_DATASETS: linked_datasets})
    else:
        new_dframe = Observation.update(dframe.join(new_column), dataset)

    return new_dframe
