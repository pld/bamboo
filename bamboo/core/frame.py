from pandas import DataFrame, Series

from bamboo.lib.constants import BAMBOO_RESERVED_KEYS, PARENT_DATASET_ID


class BambooFrame(DataFrame):
    def add_parent_column(self, parent_dataset_id):
        column = Series([parent_dataset_id] * len(self))
        column.name = PARENT_DATASET_ID
        self = self.join(column)
        return self

    def remove_bamboo_reserved_keys(self, keep_parent_ids=False):
        reserved_keys = list(set(BAMBOO_RESERVED_KEYS).intersection(
            set(self.columns.tolist())))
        if keep_parent_ids and PARENT_DATASET_ID in reserved_keys:
            reserved_keys.remove(PARENT_DATASET_ID)
        for column in reserved_keys:
            del self[column]
