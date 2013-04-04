from pandas import concat

from bamboo.core.aggregations import AGGREGATIONS
from bamboo.core.frame import BambooFrame


class Aggregator(object):
    """Perform a aggregations on datasets.

    Apply the `aggregation` to group columns by `groups` and the `columns`
    of the `dframe`. Store the resulting `dframe` as a linked dataset for
    `dataset`. If a linked dataset with the same groups already exists update
    this dataset.  Otherwise create a new linked dataset.
    """

    def __init__(self, dataset, dframe, groups, _type, name, columns):
        """Create an Aggregator.

        :param columns: The columns to aggregate over.
        :param dataset: The dataset to aggregate.
        :param dframe: The DataFrame to aggregate.
        :param groups: A list of columns to group on.
        :param _type: The aggreagtion to perform.
        :param name: The name of the aggregation.
        """
        self.columns = columns
        self.dataset = dataset
        self.dframe = dframe
        self.groups = groups
        self.name = name
        aggregation = AGGREGATIONS.get(_type)
        self.aggregation = aggregation(self.name, self.groups, self.dframe)

    def save(self):
        """Save this aggregation.

        If an aggregated dataset for this aggregations group already exists
        store in this dataset, if not create a new aggregated dataset and store
        the aggregation in this new aggregated dataset.

        """
        new_dframe = BambooFrame(self.aggregation.eval(self.columns))
        new_dframe = new_dframe.add_parent_column(self.dataset.dataset_id)

        agg_dataset = self.dataset.aggregated_dataset(self.groups)

        if agg_dataset is None:
            agg_dataset = self.dataset.new_agg_dataset(
                new_dframe, self.groups)
        else:
            agg_dframe = agg_dataset.dframe()
            new_dframe = self.__merge_dframes([agg_dframe, new_dframe])
            agg_dataset.replace_observations(new_dframe)

        self.new_dframe = new_dframe

    def update(self, child_dataset, calculator, formula):
        """Attempt to reduce an update and store."""
        parent_dataset_id = self.dataset.dataset_id

        # get dframe only including rows from this parent
        dframe = child_dataset.dframe(
            keep_parent_ids=True,
            reload_=True).only_rows_for_parent_id(parent_dataset_id)

        # remove rows in child from parent
        child_dataset.remove_parent_observations(parent_dataset_id)

        if not self.groups and 'reduce' in dir(self.aggregation):
            # if it is not grouped and a reduce is defined
            dframe = BambooFrame(
                self.aggregation.reduce(dframe, self.columns))
        else:
            dframe = self.__dframe_from_calculator(calculator, formula, dframe)

        new_agg_dframe = concat([child_dataset.dframe(), dframe])
        new_agg_dframe = new_agg_dframe.add_parent_column(parent_dataset_id)
        child_dataset.replace_observations(new_agg_dframe)

        return child_dataset.dframe()

    def __dframe_from_calculator(self, calculator, formula, dframe):
        """Create a new aggregation and update return updated dframe."""
        # build column arguments from original dframe
        columns = calculator.parse_columns(
            formula, self.name, self.dframe)
        new_dframe = self.aggregation.eval(columns)

        new_columns = [x for x in new_dframe.columns if x not in self.groups]

        dframe = dframe.drop(new_columns, axis=1)
        dframe = self.__merge_dframes([new_dframe, dframe])

        return dframe

    def __merge_dframes(self, dframes):
        if self.groups:
            # set indexes on new dataframes to merge correctly
            dframes = [dframe.set_index(self.groups) for dframe in dframes]

        # attach new column to aggregation data frame and remove index
        new_dframe = dframes[0].join(dframes[1:])

        if self.groups:
            new_dframe = new_dframe.reset_index()

        return new_dframe
