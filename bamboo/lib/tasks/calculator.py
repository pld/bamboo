from collections import defaultdict

from celery.task import task
from pandas import concat, DataFrame, Series

from lib.aggregator import Aggregator
from lib.constants import MONGO_RESERVED_KEYS
from lib.parser import Parser
from lib.utils import recognize_dates, recognize_dates_from_schema
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
    aggregation, functions = parser.parse_formula(formula)

    new_columns = []
    for function in functions:
        new_column = dframe.apply(function, axis=1, args=(parser.context, ))
        new_columns.append(new_column)

    new_columns[0].name = name

    if aggregation:
        new_dframe = Aggregator(
            dataset, dframe, new_columns, group_str, aggregation, name
        ).new_dframe

    else:
        new_dframe = Observation.update(dframe.join(new_columns[0], dataset))

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

    # make a single-row dataframe for the additional data to add
    labels_to_slugs = dataset.build_labels_to_slugs()
    filtered_data = dict()
    for col, val in new_data.iteritems():
        if labels_to_slugs.get(col, None) in existing_dframe.columns:
            filtered_data[labels_to_slugs[col]] = val
        # special case for reserved keys (e.g. _id)
        if col in MONGO_RESERVED_KEYS and\
            col in existing_dframe.columns and\
                col not in filtered_data.keys():
            filtered_data[col] = val
    new_dframe = recognize_dates_from_schema(dataset,
                                             DataFrame([filtered_data]))

    # calculate columns
    parser = Parser(dataset.record)

    # extract info from linked datasets
    labels_to_slugs_and_groups = dict()
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
        potential_name = calculation.name
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
