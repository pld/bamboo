from pandas import DataFrame, Series


# reserved bamboo keys
BAMBOO_RESERVED_KEY_PREFIX = 'BAMBOO_RESERVED_KEY_'
DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_id'
DATASET_OBSERVATION_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_observation_id'
PARENT_DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'parent_dataset_id'
BAMBOO_RESERVED_KEYS = [
    DATASET_ID,
    DATASET_OBSERVATION_ID,
    PARENT_DATASET_ID,
]


class BambooFrame(DataFrame):
    def add_parent_column(self, parent_dataset_id):
        column = Series([parent_dataset_id] * len(self))
        column.name = PARENT_DATASET_ID
        return self.join(column)

    def remove_bamboo_reserved_keys(self, keep_parent_ids=False):
        reserved_keys = list(set(BAMBOO_RESERVED_KEYS).intersection(
            set(self.columns.tolist())))
        if keep_parent_ids and PARENT_DATASET_ID in reserved_keys:
            reserved_keys.remove(PARENT_DATASET_ID)
        for column in reserved_keys:
            del self[column]
