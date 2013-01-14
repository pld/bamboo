from pandas import concat

from bamboo.core.aggregations import AGGREGATIONS
from bamboo.core.frame import BambooFrame


class Aggregator(object):
    """Perform a aggregations on datasets.

    Apply the `aggregation` to group columns in `group_str` and the `columns`
    of the `dframe`. Store the resulting `dframe` as a linked dataset for
    `dataset`. If a linked dataset with the same groups already exists update
    this dataset.  Otherwise create a new linked dataset.
    """

    def __init__(self, dataset, dframe, groups, _type, name):
        """Create an Aggregator.

        :param dataset: The dataset to aggregate.
        :param dframe: The DataFrame to aggregate.
        :param groups: A list of columns to group on.
        :param _type: The aggreagtion to perform.
        :param name: The name of the aggregation.
        """
        self.dataset = dataset
        self.dframe = dframe
        self.groups = groups
        self.name = name
        aggregation = AGGREGATIONS.get(_type)
        self.aggregation = aggregation(self.name, self.groups, self.dframe)

    def save(self, columns):
        """Save this aggregation applied to the passed in columns.

        If an aggregated dataset for this aggregations group already exists
        store in this dataset, if not create a new aggregated dataset and store
        the aggregation in this new aggregated dataset.

        :param columns: The column aggregate.
        """
        new_dframe = BambooFrame(self.aggregation.eval(columns))
        new_dframe = new_dframe.add_parent_column(self.dataset.dataset_id)

        agg_dataset = self.dataset.aggregated_dataset(self.groups)

        if agg_dataset is None:
            agg_dataset = self.dataset.new_agg_dataset(
                new_dframe, self.groups)
        else:
            agg_dframe = agg_dataset.dframe()
            new_dframe = self._merge_dframes([agg_dframe, new_dframe])
            agg_dataset.replace_observations(new_dframe)

        self.new_dframe = new_dframe

    def update(self, child_dataset, calculator, formula, columns):
        """Attempt to reduce an update and store."""
        parent_dataset_id = self.dataset.dataset_id

        # get dframe only including rows from this parent
        dframe = child_dataset.dframe(
            keep_parent_ids=True).only_rows_for_parent_id(parent_dataset_id)

        # remove rows in child from parent
        child_dataset.remove_parent_observations(parent_dataset_id)

        if not self.groups and 'reduce' in dir(self.aggregation):
            # if it is not grouped and a reduce is defined
            dframe = BambooFrame(
                self.aggregation.reduce(dframe, columns))
        else:
            dframe = self._dframe_from_calculator(calculator, formula, dframe)

        new_agg_dframe = concat([child_dataset.dframe(), dframe])

        return child_dataset.replace_observations(
            new_agg_dframe.add_parent_column(parent_dataset_id))

    def _dframe_from_calculator(self, calculator, formula, dframe):
        """Create a new aggregation and update return updated dframe."""
        # build column arguments from original dframe
        _, columns = calculator.make_columns(
            formula, self.name, self.dframe)
        new_dframe = self.aggregation.eval(columns)

        del dframe[self.name]
        dframe = self._merge_dframes(
            [new_dframe[self.groups + [self.name]], dframe])

        return dframe

    def _merge_dframes(self, dframes):
        if self.groups:
            # set indexes on new dataframes to merge correctly
            dframes = [dframe.set_index(self.groups) for dframe in dframes]

        # attach new column to aggregation data frame and remove index
        new_dframe = dframes[0].join(dframes[1:])

        if self.groups:
            new_dframe = new_dframe.reset_index()

        return new_dframe
