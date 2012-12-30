from cStringIO import StringIO

from pandas import DataFrame, Series

from bamboo.lib.datetools import recognize_dates, recognize_dates_from_schema
from bamboo.lib.jsontools import series_to_jsondict
from bamboo.lib.mongo import dump_mongo_json, mongo_prefix_reserved_key,\
    MONGO_RESERVED_KEYS


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


class NonUniqueJoinError(Exception):
    pass


class BambooFrame(DataFrame):
    """Add bamboo related functionality to DataFrame class."""

    def add_parent_column(self, parent_dataset_id):
        """Add parent ID column to this DataFrame."""
        column = Series([parent_dataset_id] * len(self))
        column.name = PARENT_DATASET_ID
        return self.__class__(self.join(column))

    def decode_mongo_reserved_keys(self):
        """Decode MongoDB reserved keys in this DataFrame."""
        reserved_keys = self._column_intersect(MONGO_RESERVED_KEYS)
        rename_dict = {}

        for key in reserved_keys:
            del self[key]
            prefixed_key = mongo_prefix_reserved_key(key)
            if prefixed_key in self.columns:
                rename_dict[prefixed_key] = key

        if rename_dict:
            self.rename(columns={prefixed_key: key}, inplace=True)

    def recognize_dates(self):
        return recognize_dates(self)

    def recognize_dates_from_schema(self, schema):
        return recognize_dates_from_schema(self, schema)

    def remove_bamboo_reserved_keys(self, keep_parent_ids=False):
        """Remove reserved internal columns in this DataFrame.

        :param keep_parent_ids: Keep parent column if True, default False.
        """
        reserved_keys = self._column_intersect(BAMBOO_RESERVED_KEYS)

        if keep_parent_ids and PARENT_DATASET_ID in reserved_keys:
            reserved_keys.remove(PARENT_DATASET_ID)

        for column in reserved_keys:
            del self[column]

    def only_rows_for_parent_id(self, parent_id):
        """DataFrame with only rows for `parent_id`.

        :param parent_id: The ID to restrict rows to.

        :returns: A DataFrame including only rows with a parent ID equal to
            that passed in.
        """
        return self[self[PARENT_DATASET_ID] == parent_id].drop(
            PARENT_DATASET_ID, 1)

    def to_jsondict(self):
        """Return DataFrame as a list of dicts for each row."""
        return [series_to_jsondict(series) for _, series in self.iterrows()]

    def to_json(self):
        """Convert DataFrame to a list of dicts, then dump to JSON."""
        jsondict = self.to_jsondict()
        return dump_mongo_json(jsondict)

    def to_csv_as_string(self):
        buffer = StringIO()
        self.to_csv(buffer, encoding='utf-8', index=False)
        return buffer.getvalue()

    def _column_intersect(self, _list):
        """Return the intersection of `_list` and this DataFrame's columns."""
        return list(set(_list).intersection(set(self.columns.tolist())))

    def join_dataset(self, other, on):
        """Left join an `other` dataset."""
        right_dframe = other.dframe(padded=True)

        if on not in self.columns:
            raise KeyError('no item named %s in left hand side dataset' % on)

        if on not in right_dframe.columns:
            raise KeyError('no item named %s in right hand side dataset' % on)

        right_dframe = right_dframe.set_index(on)

        if len(right_dframe.index) != len(right_dframe.index.unique()):
            raise NonUniqueJoinError('The join column (%s) of the right hand s'
                                     'ide dataset is not unique' % on)

        return self.__class__(self.join(right_dframe, on=on))
