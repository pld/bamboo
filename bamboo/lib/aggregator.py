from pandas import DataFrame, Series

from lib.constants import LINKED_DATASETS
from models.dataset import Dataset
from models.observation import Observation


class Aggregator(object):

    GROUP_DELIMITER = ','

    def __init__(self, dataset, dframe, column, group_str, aggregation, name):
        """
        Apply the *aggregation* to group columns in *group_str* and the *column
        of the *dframe*.
        Store the resulting *dframe* as a linked dataset for *dataset*.
        If a linked dataset with the same groups already exists update this
        dataset.  Otherwise create a new linked dataset.
        """
        self.column = column

        if group_str:
            # groupby on dframe then run aggregation on groupby obj
            groups = group_str.split(self.GROUP_DELIMITER)
            self.new_dframe = dframe[groups].join(column).\
                groupby(groups, as_index=False).agg(aggregation)
        else:
            result = self.function_map(aggregation)
            self.new_dframe = DataFrame({name: Series([result])})
            group_str = ''

        linked_datasets = dataset.linked_datasets

        # MongoDB does not allow None as a key
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

    def function_map(self, function):
        return {
            'sum': self.sum_dframe,
        }[function]()

    def sum_dframe(self):
        return float(self.column.sum())
