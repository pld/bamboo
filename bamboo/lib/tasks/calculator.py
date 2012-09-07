from collections import defaultdict

from celery.task import task
from pandas import concat, DataFrame, Series

from lib.aggregator import Aggregator
from lib.parser import Parser
from models.observation import Observation


@task
def calculate_column(parser, dataset, dframe, formula, name, group=None,
                     query=None):
    """
    For calculating new columns.
    Get necessary data given a calculation ID, execute calculation formula,
    store results in dataset the calculation refers to.

    This can result in race-conditions when:

    - deleting, ``controllers.Datasets.DELETE``
    - updating ``controllers.Datasets.POST([dataset_id])``

    Therefore, perform these actions asychronously.
    """
    # parse formula into function and variables
    aggregation, function = parser.parse_formula(formula)

    new_column = dframe.apply(function, axis=1, args=(parser, ))
    new_column.name = name

    if aggregation:
        new_dframe = Aggregator(
            dataset, dframe, new_column, group, aggregation, name
        ).new_dframe

    else:
        new_dframe = Observation.update(dframe.join(new_column), dataset)

    return new_dframe


@task
def calculate_updates(dataset, new_data, calculations, FORMULA, NAME):
    """
    Update dataset with new data.

    This can result in race-conditions when:

    - deleting, ``controllers.Datasets.DELETE``
    - updating ``controllers.Datasets.POST([dataset_id])``

    Therefore, perform these actions asychronously.
    """
    # get the dataframe for this dataset
    existing_dframe = Observation.find(dataset, as_df=True)

    # make a dataframe for the additional data to add
    filtered_data = [dict([(k, v) for k, v in new_data.iteritems()
                     if k in existing_dframe.columns])]
    new_dframe = DataFrame(filtered_data)

    # calculate columns
    # TODO update aggregated datasets
    parser = Parser(dataset.record)
    labels_to_slugs = dataset.build_labels_to_slugs()

    for calculation in calculations:
        aggregation, function = \
            parser.parse_formula(calculation.record[FORMULA])
        new_column = new_dframe.apply(function, axis=1,
                                      args=(parser, ))
        potential_name = calculation.record[NAME]
        if potential_name not in existing_dframe.columns:
            new_column.name = labels_to_slugs[potential_name]
        else:
            new_column.name = potential_name
        new_dframe = new_dframe.join(new_column)

    # merge the two
    updated_dframe = concat([existing_dframe, new_dframe])

    # update (overwrite) the dataset with the new merged version
    updated_dframe = Observation.update(updated_dframe, dataset)

    dataset.clear_summary_stats(ALL)
