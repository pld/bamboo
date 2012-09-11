from collections import defaultdict

from celery.task import task
from pandas import concat, DataFrame, Series

from lib.aggregator import Aggregator
from lib.parser import Parser
from models.observation import Observation
from models.dataset import Dataset


@task
def calculate_column(parser, dataset, dframe, formula, name, group_str=None):
    """
    Calculate a new column based on *formula* store as *name*.
    The *fomula* is parsed by the *parser* and applied to *dframe*.
    The new column is joined to *dframe* and stored in *dataset*.
    The *group* is only applicable to aggregations and groups for aggregations.

    This can result in race-conditions when:

    - deleting ``controllers.Datasets.DELETE``
    - updating ``controllers.Datasets.POST([dataset_id])``

    Therefore, perform these actions asychronously.
    """
    # parse formula into function and variables
    aggregation, function = parser.parse_formula(formula)

    new_column = dframe.apply(function, axis=1, args=(parser, ))
    new_column.name = name

    if aggregation:
        new_dframe = Aggregator(
            dataset, dframe, new_column, group_str, aggregation, name
        ).new_dframe

    else:
        new_dframe = Observation.update(dframe.join(new_column), dataset)

    return new_dframe


@task
def calculate_updates(dataset, new_data, calculations, FORMULA, NAME):
    """
    Update dataset with *new_data*.

    This can result in race-conditions when:

    - deleting ``controllers.Datasets.DELETE``
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
    parser = Parser(dataset.record)
    labels_to_slugs = dataset.build_labels_to_slugs()

    labels_to_slugs_and_groups = dict()

    # extract info from linked datasets
    for group, dataset_id in dataset.linked_datasets.items():
        linked_dataset = Dataset.find_one(dataset_id)
        for label, slug in linked_dataset.build_labels_to_slugs().items():
            labels_to_slugs_and_groups[label] = (slug, group)
        linked_dataset.delete(linked_dataset)

    # remove linked datasets
    dataset.clear_linked_datasets()

    for calculation in calculations:
        aggregation, function = \
            parser.parse_formula(calculation.record[FORMULA])
        new_column = new_dframe.apply(function, axis=1,
                                      args=(parser, ))
        potential_name = calculation.record[NAME]
        if potential_name not in existing_dframe.columns:
            if potential_name not in labels_to_slugs:
                # it is a linked calculation
                slug, group = labels_to_slugs_and_groups[potential_name]
                calculate_column(parser, dataset, existing_dframe,
                                 potential_name, slug, group)
                continue
            else:
                new_column.name = labels_to_slugs[potential_name]
        else:
            new_column.name = potential_name
        new_dframe = new_dframe.join(new_column)

    # merge the two
    updated_dframe = concat([existing_dframe, new_dframe])

    # update (overwrite) the dataset with the new merged version
    updated_dframe = Observation.update(updated_dframe, dataset)

    dataset.clear_summary_stats()
