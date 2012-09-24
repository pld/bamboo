from pandas import DataFrame, Series

from lib.aggregations import Aggregation
from lib.constants import LINKED_DATASETS
from lib.utils import split_groups
from models.dataset import Dataset
from models.observation import Observation


class Aggregator(object):

    AGGREGATIONS = dict([(cls.name, cls()) for cls in
                         Aggregation.__subclasses__()])

    def __init__(self, dataset, dframe, column, group_str, _type,
                 name):
        """
        Apply the *aggregation* to group columns in *group_str* and the *column
        of the *dframe*.
        Store the resulting *dframe* as a linked dataset for *dataset*.
        If a linked dataset with the same groups already exists update this
        dataset.  Otherwise create a new linked dataset.
        """
        self.new_dframe = self._eval(_type, group_str, name, dframe, column)

        if not group_str:
            # MongoDB does not allow None as a key
            group_str = ''

        linked_datasets = dataset.linked_datasets
        agg_dataset_id = linked_datasets.get(group_str, None)

        if agg_dataset_id is None:
            agg_dataset = Dataset()
            agg_dataset.save()

            Observation().save(self.new_dframe, agg_dataset)

            # store a link to the new dataset
            linked_datasets[group_str] = agg_dataset.dataset_id
            dataset.update({LINKED_DATASETS: linked_datasets})
        else:
            agg_dataset = Dataset.find_one(agg_dataset_id)
            agg_dframe = Observation.find(agg_dataset, as_df=True)

            # attach new column to aggregation data frame
            self.new_dframe = agg_dframe.join(self.new_dframe[name])
            Observation.update(self.new_dframe, agg_dataset)

    def _eval(self, _type, group_str, name, dframe, column):
        aggregation = self.AGGREGATIONS.get(_type)

        if group_str:
            # groupby on dframe then run aggregation on groupby obj
            groups = split_groups(group_str)
            return aggregation.group_aggregation(dframe[groups].join(
                column).groupby(groups, as_index=False))
        else:
            result = aggregation.column_aggregation(column)
            return DataFrame({name: Series([result])})
