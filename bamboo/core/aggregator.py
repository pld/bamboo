from pandas import concat, Series

from bamboo.core.aggregations import Aggregation, AGGREGATIONS
from bamboo.core.frame import BambooFrame
from bamboo.lib.utils import split_groups


class Aggregator(object):
    """
    Apply the *aggregation* to group columns in *group_str* and the *columns*
    of the *dframe*.
    Store the resulting *dframe* as a linked dataset for *dataset*.
    If a linked dataset with the same groups already exists update this
    dataset.  Otherwise create a new linked dataset.
    """

    def __init__(self, dataset, dframe, group_str, _type, name):
        self.dataset = dataset
        self.dframe = dframe
        # MongoDB does not allow None as a key
        self.group_str = group_str if group_str else ''
        self.groups = split_groups(self.group_str) if group_str else None
        self.name = name
        self.aggregation = AGGREGATIONS.get(_type)(
            self.name, self.groups, self.dframe)

    def save(self, columns):
        """
        Save this aggregation applied to the passed in columns.  If an
        aggregated dataset for this aggregations group already exists store in
        this dataset, if not create a new aggregated dataset and store the
        aggregation in this new aggregated dataset.
        """
        new_dframe = BambooFrame(
            self.aggregation._eval(columns)
        ).add_parent_column(self.dataset.dataset_id)

        aggregated_datasets = self.dataset.aggregated_datasets
        agg_dataset = aggregated_datasets.get(self.group_str, None)

        if agg_dataset is None:
            agg_dataset = self.dataset.__class__()
            agg_dataset.save()
            agg_dataset.save_observations(new_dframe)

            # store a link to the new dataset
            aggregated_datasets_dict = self.dataset.aggregated_datasets_dict
            aggregated_datasets_dict[self.group_str] = agg_dataset.dataset_id
            self.dataset.update({
                self.dataset.AGGREGATED_DATASETS: aggregated_datasets_dict})
        else:
            agg_dframe = agg_dataset.dframe()

            if self.groups:
                # set indexes on new dataframes to merge correctly
                new_dframe = new_dframe.set_index(self.groups)
                agg_dframe = agg_dframe.set_index(self.groups)

            # attach new column to aggregation data frame and remove index
            new_dframe = agg_dframe.join(new_dframe)

            if self.groups:
                new_dframe = new_dframe.reset_index()

            agg_dataset.replace_observations(new_dframe)
        self.new_dframe = new_dframe

    def update(self, child_dataset, parser, formula, columns):
        """
        Attempt to reduce an update and store.
        """
        parent_dataset_id = self.dataset.dataset_id

        # get dframe only including rows from this parent
        dframe = child_dataset.dframe(
            keep_parent_ids=True).only_rows_for_parent_id(parent_dataset_id)

        # remove rows in child from parent
        child_dataset.remove_parent_observations(parent_dataset_id)

        if not self.groups and '_reduce' in dir(self.aggregation):
            dframe = BambooFrame(
                self.aggregation._reduce(dframe, columns))
        else:
            _, columns = parser._make_columns(
                formula, self.name, self.dframe)
            new_dframe = self.aggregation._eval(columns)
            dframe[self.name] = new_dframe[self.name]

        new_agg_dframe = concat([child_dataset.dframe(), dframe])
        return child_dataset.replace_observations(
            new_agg_dframe.add_parent_column(parent_dataset_id))
