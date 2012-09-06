from pandas import DataFrame, Series

from lib.constants import LINKED_DATASETS
from models.dataset import Dataset
from models.observation import Observation


class Aggregator(object):

    def __init__(self, dataset, dframe, column, group, aggregation, name):
        self.column = column

        if group:
            # groupby on dframe then run aggregation on groupby obj
            self.new_dframe = DataFrame(dframe[group]).join(column).\
                groupby(group, as_index=False).agg(aggregation)
        else:
            result = self.function_map(aggregation)
            self.new_dframe = DataFrame({name: Series([result])})

        linked_datasets = dataset.linked_datasets

        # MongoDB does not allow None as a key
        agg_dataset_id = linked_datasets.get(group or '', None)

        if agg_dataset_id is None:
            agg_dataset = Dataset()
            agg_dataset.save()

            Observation().save(self.new_dframe, agg_dataset)

            # store a link to the new dataset
            linked_datasets[group or ''] = agg_dataset.dataset_id
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
