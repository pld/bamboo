from collections import defaultdict

from celery.task import task
from pandas import concat, DataFrame

from bamboo.core.aggregator import Aggregator
from bamboo.core.frame import BambooFrame, NonUniqueJoinError
from bamboo.core.parser import Parser
from bamboo.lib.mongo import MONGO_ID
from bamboo.lib.parsing import parse_columns
from bamboo.lib.query_args import QueryArgs
from bamboo.lib.utils import combine_dicts, flatten, to_list


def calculate_columns(dataset, calculations):
    """Calculate and store new columns for `calculations`.

    The new columns are join t othe Calculation dframe and replace the
    dataset's observations.

    .. note::

        This can result in race-conditions when:

        - deleting ``controllers.Datasets.DELETE``
        - updating ``controllers.Datasets.POST([dataset_id])``

        Therefore, perform these actions asychronously.

    :param dataset: The dataset to calculate for.
    :param calculations: A list of calculations.
    """
    new_cols = None

    for c in calculations:
        if c.aggregation:
            aggregator = __create_aggregator(
                dataset, c.formula, c.name, c.groups_as_list)
            aggregator.save(dataset)
        else:
            columns = parse_columns(dataset, c.formula, c.name)
            if new_cols is None:
                new_cols = DataFrame(columns[0])
            else:
                new_cols = new_cols.join(columns[0])

    if new_cols is not None:
        dataset.update_observations(new_cols)

    # propagate calculation to any merged child datasets
    [__propagate_column(x, dataset) for x in dataset.merged_datasets]


@task(default_retry_delay=5, ignore_result=True)
def calculate_updates(dataset, new_data, new_dframe_raw=None,
                      parent_dataset_id=None, update_id=None):
    """Update dataset with `new_data`.

    This can result in race-conditions when:

    - deleting ``controllers.Datasets.DELETE``
    - updating ``controllers.Datasets.POST([dataset_id])``

    Therefore, perform these actions asychronously.

    :param new_data: Data to update this dataset with.
    :param new_dframe_raw: DataFrame to update this dataset with.
    :param parent_dataset_id: If passed add ID as parent ID to column,
        default is None.
    """
    __ensure_ready(dataset, update_id)

    if new_dframe_raw is None:
        new_dframe_raw = dframe_from_update(dataset, new_data)

    new_dframe = new_dframe_raw.recognize_dates_from_schema(dataset.schema)

    new_dframe = __add_calculations(dataset, new_dframe)

    # set parent id if provided
    if parent_dataset_id:
        new_dframe = new_dframe.add_parent_column(parent_dataset_id)

    dataset.append_observations(new_dframe)
    dataset.clear_summary_stats()

    propagate(dataset, new_dframe=new_dframe, update={'add': new_dframe_raw})

    dataset.update_complete(update_id)


def dframe_from_update(dataset, new_data):
    """Make a DataFrame for the `new_data`.

    :param new_data: Data to add to dframe.
    :type new_data: List.
    """
    filtered_data = []
    columns = dataset.columns
    labels_to_slugs = dataset.schema.labels_to_slugs
    num_columns = len(columns)
    num_rows = dataset.num_rows
    dframe_empty = not num_columns

    if dframe_empty:
        columns = dataset.schema.keys()

    for row in new_data:
        filtered_row = dict()
        for col, val in row.iteritems():
            # special case for reserved keys (e.g. _id)
            if col == MONGO_ID:
                if (not num_columns or col in columns) and\
                        col not in filtered_row.keys():
                    filtered_row[col] = val
            else:
                # if col is a label take slug, if it's a slug take col
                slug = labels_to_slugs.get(
                    col, col if col in labels_to_slugs.values() else None)

                # if slug is valid or there is an empty dframe
                if (slug or col in labels_to_slugs.keys()) and (
                        dframe_empty or slug in columns):
                    filtered_row[slug] = dataset.schema.convert_type(
                        slug, val)

        filtered_data.append(filtered_row)

    index = range(num_rows, num_rows + len(filtered_data))
    new_dframe = BambooFrame(filtered_data, index=index)
    __check_update_is_valid(dataset, new_dframe)

    return new_dframe


@task(default_retry_delay=5, ignore_result=True)
def propagate(dataset, new_dframe=None, update=None):
    """Propagate changes in a modified dataset."""
    __update_aggregate_datasets(dataset, new_dframe, update=update)

    if update:
        __update_merged_datasets(dataset, update)
        __update_joined_datasets(dataset, update)


def __add_calculations(dataset, new_dframe):
    labels_to_slugs = dataset.schema.labels_to_slugs

    for calculation in dataset.calculations(include_aggs=False):
        function = Parser.parse_function(calculation.formula)
        new_column = new_dframe.apply(function, axis=1,
                                      args=(dataset, ))
        potential_name = calculation.name

        if potential_name not in dataset.dframe().columns:
            if potential_name in labels_to_slugs:
                new_column.name = labels_to_slugs[potential_name]
        else:
            new_column.name = potential_name

        new_dframe = new_dframe.join(new_column)

    return new_dframe


def __calculation_data(dataset):
    """Create a list of aggregate calculation information.

    Builds a list of calculation information from the current datasets
    aggregated datasets and aggregate calculations.
    """
    calcs_to_data = defaultdict(list)

    calculations = dataset.calculations(only_aggs=True)
    names_to_formulas = {c.name: c.formula for c in calculations}
    names = set(names_to_formulas.keys())

    for group, dataset in dataset.aggregated_datasets:
        labels_to_slugs = dataset.schema.labels_to_slugs
        calculations_for_dataset = list(set(
            labels_to_slugs.keys()).intersection(names))

        for calc in calculations_for_dataset:
            calcs_to_data[calc].append((
                names_to_formulas[calc],
                labels_to_slugs[calc],
                group,
                dataset
            ))

    return flatten(calcs_to_data.values())


def __check_update_is_valid(dataset, new_dframe_raw):
    """Check if the update is valid.

    Check whether this is a right-hand side of any joins
    and deny the update if the update would produce an invalid
    join as a result.

    :raises: `NonUniqueJoinError` if update is illegal given joins of
        dataset.
    """
    select = {on: 1 for on in dataset.on_columns_for_rhs_of_joins if on in
              new_dframe_raw.columns and on in dataset.columns}
    dframe = dataset.dframe(query_args=QueryArgs(select=select))

    for on in select.keys():
        merged_join_column = concat([new_dframe_raw[on], dframe[on]])

        if len(merged_join_column) != merged_join_column.nunique():
            raise NonUniqueJoinError(
                'Cannot update. This is the right hand join and the'
                'column "%s" will become non-unique.' % on)


def __create_aggregator(dataset, formula, name, groups, dframe=None):
    # TODO this should work with index eventually
    columns = parse_columns(
        dataset, formula, name, dframe, no_index=True)

    dependent_columns = Parser.dependent_columns(formula, dataset)
    aggregation = Parser.parse_aggregation(formula)

    # get dframe with only the necessary columns
    select = combine_dicts({group: 1 for group in groups},
                           {col: 1 for col in dependent_columns})

    # ensure at least one column (MONGO_ID) for the count aggregation
    query_args = QueryArgs(select=select or {MONGO_ID: 1})
    dframe = dataset.dframe(query_args=query_args,
                            keep_mongo_keys=not select)

    return Aggregator(dframe, groups, aggregation, name, columns)


def __ensure_ready(dataset, update_id):
    # dataset must not be pending
    if not dataset.is_ready or (
            update_id and dataset.has_pending_updates(update_id)):
        dataset.reload()
        raise calculate_updates.retry()


def __find_merge_offset(dataset, merged_dataset):
    offset = 0

    for parent_id in merged_dataset.parent_ids:
        if dataset.dataset_id == parent_id:
            break

        offset += dataset.find_one(parent_id).num_rows

    return offset


def __propagate_column(dataset, parent_dataset):
    """Propagate columns in `parent_dataset` to `dataset`.

    When a new calculation is added to a dataset this will propagate the
    new column to all child (merged) datasets.

    :param dataset: THe child dataet.
    :param parent_dataset: The dataset to propagate.
    """
    # delete the rows in this dataset from the parent
    dataset.remove_parent_observations(parent_dataset.dataset_id)

    # get this dataset without the out-of-date parent rows
    dframe = dataset.dframe(keep_parent_ids=True)

    # create new dframe from the upated parent and add parent id
    parent_dframe = parent_dataset.dframe().add_parent_column(
        parent_dataset.dataset_id)

    # merge this new dframe with the existing dframe
    updated_dframe = concat([dframe, parent_dframe])

    # save new dframe (updates schema)
    dataset.replace_observations(updated_dframe)
    dataset.clear_summary_stats()

    # recur into merged dataset
    [__propagate_column(x, dataset) for x in dataset.merged_datasets]


def __remapped_data(dataset_id, mapping, slugified_data):
    column_map = mapping.get(dataset_id) if mapping else None

    if column_map:
        slugified_data = [{column_map.get(k, k): v for k, v in row.items()}
                          for row in slugified_data]

    return slugified_data


def __slugify_data(new_data, labels_to_slugs):
    slugified_data = []
    new_data = to_list(new_data)

    for row in new_data:
        for key, value in row.iteritems():
            if labels_to_slugs.get(key) and key != MONGO_ID:
                del row[key]
                row[labels_to_slugs[key]] = value

        slugified_data.append(row)

    return slugified_data


def __update_aggregate_datasets(dataset, new_dframe, update=None):
    calcs_to_data = __calculation_data(dataset)

    for formula, slug, groups, a_dataset in calcs_to_data:
        __update_aggregate_dataset(dataset, formula, new_dframe, slug, groups,
                                   a_dataset, update is None)


def __update_aggregate_dataset(dataset, formula, new_dframe, name, groups,
                               a_dataset, reducible):
    """Update the aggregated dataset built for `dataset` with `calculation`.

    Proceed with the following steps:

        - delete the rows in this dataset from the parent
        - recalculate aggregated dataframe from aggregation
        - update aggregated dataset with new dataframe and add parent id
        - recur on all merged datasets descending from the aggregated
          dataset

    :param formula: The formula to execute.
    :param new_dframe: The DataFrame to aggregate on.
    :param name: The name of the aggregation.
    :param groups: A column or columns to group on.
    :type group: String, list of strings, or None.
    :param a_dataset: The DataSet to store the aggregation in.
    """
    # parse aggregation and build column arguments
    aggregator = __create_aggregator(
        dataset, formula, name, groups, dframe=new_dframe)
    new_agg_dframe = aggregator.update(dataset, a_dataset, formula, reducible)

    # jsondict from new dframe
    new_data = new_agg_dframe.to_jsondict()

    for merged_dataset in a_dataset.merged_datasets:
        # remove rows in child from this merged dataset
        merged_dataset.remove_parent_observations(a_dataset.dataset_id)

        # calculate updates for the child
        calculate_updates(merged_dataset, new_data,
                          parent_dataset_id=a_dataset.dataset_id)


def __update_joined_datasets(dataset, update):
    """Update any joined datasets."""
    if 'add' in update:
        new_dframe = update['add']

    for direction, other_dataset, on, j_dataset in dataset.joined_datasets:
        if 'add' in update:
            if direction == 'left':
                # only proceed if on in new dframe
                if on in new_dframe.columns:
                    left_dframe = other_dataset.dframe(padded=True)

                    # only proceed if new on value is in on column in lhs
                    if len(set(new_dframe[on]).intersection(
                            set(left_dframe[on]))):
                        merged_dframe = left_dframe.join_dataset(dataset, on)
                        j_dataset.replace_observations(merged_dframe)

                        # TODO is it OK not to propagate the join here?
            else:
                # if on in new data join with existing data
                if on in new_dframe:
                    new_dframe = new_dframe.join_dataset(other_dataset, on)

                calculate_updates(j_dataset, new_dframe.to_jsondict(),
                                  parent_dataset_id=dataset.dataset_id)
        elif 'delete' in update:
            j_dataset.delete_observation(update['delete'])
        elif 'edit' in update:
            j_dataset.update_observation(*update['edit'])
            #print j_dataset.dframe()


def __update_merged_datasets(dataset, update):
    if 'add' in update:
        data = update['add'].to_jsondict()

        # store slugs as labels for child datasets
        data = __slugify_data(data, dataset.schema.labels_to_slugs)

    # update the merged datasets with new_dframe
    for mapping, merged_dataset in dataset.merged_datasets_with_map:
        if 'add' in update:
            mapped_data = __remapped_data(dataset.dataset_id, mapping, data)
            calculate_updates(merged_dataset, mapped_data,
                              parent_dataset_id=dataset.dataset_id)
        elif 'delete' in update:
            offset = __find_merge_offset(dataset, merged_dataset)
            merged_dataset.delete_observation(update['delete'] + offset)
        elif 'edit' in update:
            offset = __find_merge_offset(dataset, merged_dataset)
            index, data = update['edit']
            merged_dataset.update_observation(index + offset, data)
