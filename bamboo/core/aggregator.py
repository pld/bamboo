from pandas import Series

from bamboo.core.aggregations import Aggregation, AGGREGATIONS
from bamboo.core.frame import BambooFrame
from bamboo.lib.utils import split_groups
from bamboo.models.observation import Observation


class Aggregator(object):
    """
    Apply the *aggregation* to group columns in *group_str* and the *columns*
    of the *dframe*.
    Store the resulting *dframe* as a linked dataset for *dataset*.
    If a linked dataset with the same groups already exists update this
    dataset.  Otherwise create a new linked dataset.
    """

    def __init__(self, dataset, dframe, columns, group_str, _type, name):
        self.dataset = dataset
        self.dframe = dframe
        self.columns = columns
        # MongoDB does not allow None as a key
        self.group_str = group_str if group_str else ''
        self.groups = split_groups(self.group_str) if group_str else None
        self.name = name
        self._type = _type

    def save_aggregation(self):
        new_dframe = BambooFrame(self.eval_dframe()).add_parent_column(
            self.dataset.dataset_id)

        aggregated_datasets = self.dataset.aggregated_datasets
        agg_dataset = aggregated_datasets.get(self.group_str, None)

        if agg_dataset is None:
            agg_dataset = self.dataset.__class__()
            agg_dataset.save()

            Observation().save(new_dframe, agg_dataset)

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

    def eval_dframe(self):
        aggregation = AGGREGATIONS.get(self._type)

        if self.group_str:
            # groupby on dframe then run aggregation on groupby obj
            return aggregation.group_aggregation(self.dframe, self.groups,
                                                 self.columns)
        else:
            return aggregation.column_aggregation(self.columns, self.name)
