from collections import defaultdict

from celery.task import task
from pandas import DataFrame, Series

from lib.constants import DATASET_ID, LINKED_DATASETS
from models.dataset import Dataset
from models.observation import Observation


def sum_dframe(column):
    return float(column.sum())


# TODO: move this somewhere else
FUNCTION_MAP = {
    'sum': sum_dframe,
}


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
            agg_dframe = DataFrame(dframe[group]).join(new_column).\
                groupby(group, as_index=False).agg(aggregation)
            new_column = agg_dframe[name]
        else:
            result = FUNCTION_MAP[aggregation](new_column)
            new_column = Series([result])

        linked_datasets = dataset.get(LINKED_DATASETS, {})
        # Mongo does not allow None as a key
        agg_dataset_id = linked_datasets.get(group or '', None)

        if agg_dataset_id is None:
            agg_dataset = Dataset.create()
            new_dframe = agg_dframe if group else DataFrame({name: new_column})

            Observation.save(new_dframe, agg_dataset)

            # store a link to the new dataset
            linked_datasets[group or ''] = agg_dataset[DATASET_ID]
            Dataset.update(dataset, {LINKED_DATASETS: linked_datasets})
        else:
            agg_dataset = Dataset.find_one(agg_dataset_id)
            new_dframe = Observation.find(agg_dataset, as_df=True)

            # attach new column to aggregation data frame
            new_column.name = name
            new_dframe = new_dframe.join(new_column)
            Observation.update(new_dframe, agg_dataset)
    else:
        new_dframe = Observation.update(dframe.join(new_column), dataset)

    return new_dframe
