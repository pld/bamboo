from pandas import DataFrame, Series

from lib.aggregations import Aggregation
from lib.constants import LINKED_DATASETS
from lib.utils import split_groups
from models.dataset import Dataset
from models.observation import Observation


class Aggregator(object):

    AGGREGATIONS = dict([(cls.name, cls()) for cls in
                         Aggregation.__subclasses__()])

    def __init__(
        self, dataset, dframe, column, group_str, _type, name):
        """
        Apply the *aggregation* to group columns in *group_str* and the *column
        of the *dframe*.
        Store the resulting *dframe* as a linked dataset for *dataset*.
        If a linked dataset with the same groups already exists update this
        dataset.  Otherwise create a new linked dataset.
        """
        self.dataset = dataset
        self.dframe = dframe
        self.column = column
        # MongoDB does not allow None as a key
        self.group_str = group_str if group_str else ''
        self.groups = split_groups(self.group_str) if group_str else None
        self.name = name
        self._type = _type
        self.save_aggregation()

    def save_aggregation(self):
        new_dframe = self._eval()

        linked_datasets = self.dataset.linked_datasets
        agg_dataset_id = linked_datasets.get(self.group_str, None)

        if agg_dataset_id is None:
            agg_dataset = Dataset()
            agg_dataset.save()

            Observation().save(new_dframe, agg_dataset)

            # store a link to the new dataset
            linked_datasets[self.group_str] = agg_dataset.dataset_id
            self.dataset.update({LINKED_DATASETS: linked_datasets})
        else:
            agg_dataset = Dataset.find_one(agg_dataset_id)
            agg_dframe = Observation.find(agg_dataset, as_df=True)

            if self.groups:
                # set indexes on new dataframes to merge correctly
                new_dframe = new_dframe.set_index(self.groups)
                agg_dframe = agg_dframe.set_index(self.groups)

            # attach new column to aggregation data frame and remove index
            new_dframe = agg_dframe.join(new_dframe).reset_index()
            Observation.update(new_dframe, agg_dataset)
        self.new_dframe = new_dframe

    def _eval(self):
        aggregation = self.AGGREGATIONS.get(self._type)

        if self.group_str:
            # groupby on dframe then run aggregation on groupby obj
            return aggregation.group_aggregation(self.dframe, self.groups, self.column)
        else:
            return aggregation.column_aggregation(self.column, self.name)
