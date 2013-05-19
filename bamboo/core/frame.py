from cStringIO import StringIO

from pandas import DataFrame, Series

from bamboo.lib.datetools import recognize_dates, recognize_dates_from_schema
from bamboo.lib.jsontools import series_to_jsondict
from bamboo.lib.mongo import dump_mongo_json, MONGO_ID, MONGO_ID_ENCODED


# reserved bamboo keys
BAMBOO_RESERVED_KEY_PREFIX = '^^'
DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'dataset_id'
INDEX = BAMBOO_RESERVED_KEY_PREFIX + 'index'
PARENT_DATASET_ID = BAMBOO_RESERVED_KEY_PREFIX + 'parent_dataset_id'

BAMBOO_RESERVED_KEYS = [
    DATASET_ID,
    INDEX,
    PARENT_DATASET_ID,
]

# all the reserved keys
RESERVED_KEYS = BAMBOO_RESERVED_KEYS + [MONGO_ID_ENCODED]


class NonUniqueJoinError(Exception):
    pass


class BambooFrame(DataFrame):
    """Add bamboo related functionality to DataFrame class."""

    def add_index(self):
        """Add an encoded index to this DataFrame."""
        if not INDEX in self.columns:
            # No index, create index for this dframe.

            if not 'index' in self.columns:
                # Custom index not supplied, use pandas default index.
                self = self.reset_index()

            self.rename(columns={'index': INDEX}, inplace=True)

        return self.__class__(self)

    def add_id_column(self, dataset_id):
        return self.add_constant_column(dataset_id, DATASET_ID) if not\
            DATASET_ID in self.columns else self

    def add_parent_column(self, parent_dataset_id):
        """Add parent ID column to this DataFrame."""
        return self.add_constant_column(parent_dataset_id, PARENT_DATASET_ID)

    def add_constant_column(self, value, name):
        column = Series([value] * len(self), index=self.index, name=name)
        return self.__class__(self.join(column))

    def decode_mongo_reserved_keys(self, keep_mongo_keys=False):
        """Decode MongoDB reserved keys in this DataFrame."""
        rename_dict = {}

        if MONGO_ID in self.columns:
            if keep_mongo_keys:
                self.rename(
                    columns={MONGO_ID: MONGO_ID_ENCODED,
                             MONGO_ID_ENCODED: MONGO_ID},
                    inplace=True)
            else:
                del self[MONGO_ID]
                if MONGO_ID_ENCODED in self.columns:
                    rename_dict[MONGO_ID_ENCODED] = MONGO_ID

        if rename_dict:
            self.rename(columns={MONGO_ID_ENCODED: MONGO_ID}, inplace=True)

    def recognize_dates(self):
        return recognize_dates(self)

    def recognize_dates_from_schema(self, schema):
        return recognize_dates_from_schema(self, schema)

    def remove_bamboo_reserved_keys(self, exclude=[]):
        """Remove reserved internal columns in this DataFrame.

        :param keep_parent_ids: Keep parent column if True, default False.
        """
        reserved_keys = self.__column_intersect(BAMBOO_RESERVED_KEYS)
        reserved_keys = reserved_keys.difference(set(exclude))

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

    def join_dataset(self, other, on):
        """Left join an `other` dataset.

        :param other: Other dataset to join.
        :param on: Column or 2 comma seperated columns to join on.

        :returns: Joined DataFrame.

        :raises: `KeyError` if join columns not in datasets.
        """
        on_lhs, on_rhs = (on.split(',') * 2)[:2]

        right_dframe = other.dframe(padded=True)

        if on_lhs not in self.columns:
            raise KeyError('no item named "%s" in left hand side dataset' %
                           on_lhs)

        if on_rhs not in right_dframe.columns:
            raise KeyError('no item named "%s" in right hand side dataset' %
                           on_rhs)

        right_dframe = right_dframe.set_index(on_rhs)

        if len(right_dframe.index) != len(right_dframe.index.unique()):
            raise NonUniqueJoinError('The join column "%s" of the right hand s'
                                     'ide dataset is not unique' % on_rhs)

        shared_columns = self.columns.intersection(right_dframe.columns)

        if len(shared_columns):
            rename_map = [{c: '%s.%s' % (c, v) for c in shared_columns} for v
                          in ['x', 'y']]
            self.rename(columns=rename_map[0], inplace=True)
            right_dframe.rename(columns=rename_map[1], inplace=True)

        return self.__class__(self.join(right_dframe, on=on_lhs))

    def __column_intersect(self, list_):
        """Return the intersection of `list_` and this DataFrame's columns."""
        return set(list_).intersection(set(self.columns.tolist()))
