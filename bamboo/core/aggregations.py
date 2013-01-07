from pandas import concat, DataFrame, Series

from bamboo.lib.utils import parse_float


class Aggregation(object):
    """Abstract class for all aggregations.

    :param column: Column to aggregate.
    :param columns: List of columns to aggregate.
    :param formula_name: The string to refer to this aggregation.
    """

    column = None
    columns = None
    formula_name = None

    def __init__(self, name, groups, dframe):
        self.name = name
        self.groups = groups
        self.dframe = dframe

    def eval(self, columns):
        self.columns = columns
        self.column = columns[0] if len(columns) else None
        return self.group() if self.groups else self.agg()

    def group(self):
        """For when aggregation is called with a group parameter."""
        return self._groupby().agg(self.formula_name)

    def agg(self):
        """For when aggregation is called without a group parameter."""
        result = float(self.column.__getattribute__(self.formula_name)())
        return self._value_to_dframe(result)

    def _value_to_dframe(self, value):
        return DataFrame({self.name: Series([value])})

    def _groupby(self):
        return self.dframe[self.groups].join(
            self.column).groupby(self.groups, as_index=False)


class MultiColumnAggregation(Aggregation):
    """Interface for aggregations that create multiple columns."""

    def group(self, columns):
        dframe = self.dframe[self.groups]
        dframe = self._build_dframe(dframe, columns)
        groupby = dframe.groupby(self.groups, as_index=False)
        return self._add_calculated_column(groupby.sum())

    def reduce(self, dframe, columns):
        """Reduce the columns and store in `dframe`.

        :param dframe: The DataFrame to reduce.
        :param columns: Columns in the DataFrame to reduce on.
        """
        self.columns = columns
        self.column = columns[0]
        new_dframe = self.agg()

        for column in new_dframe.columns:
            dframe[column] += new_dframe[column]

        dframe[self.name] = self._agg_dframe(dframe)

        return dframe

    def _name_for_idx(self, idx):
        return '%s_%s' % (self.name, {
            0: 'numerator',
            1: 'denominator',
        }[idx])

    def _build_dframe(self, dframe, columns):
        for idx, column in enumerate(columns):
            column.name = self._name_for_idx(idx)

        return concat([dframe] + [DataFrame(col) for col in columns], axis=1)

    def _add_calculated_column(self, dframe):
        column = dframe[self._name_for_idx(0)].apply(float) /\
            dframe[self._name_for_idx(1)]
        column.name = self.name

        return dframe.join(column)

    def _agg_dframe(self, dframe):
        return dframe[self._name_for_idx(0)].apply(float) /\
            dframe[self._name_for_idx(1)]


class MaxAggregation(Aggregation):
    """Calculate the maximum.

    Written as ``max(FORMULA)``. Where `FORMULA` is a valid formula.
    """

    formula_name = 'max'


class ArgMaxAggregation(Aggregation):
    """Return the index for the maximum of a column.

    Written as ``argmax(FORMULA)``. Where `FORMULA` is a valid formula.
    """

    formula_name = 'argmax'

    def group(self):
        """For when aggregation is called with a group parameter."""
        self.column = self.column.apply(lambda value: parse_float(value))
        group_dframe = self.dframe[self.groups].join(self.column)
        indices = group_dframe.reset_index().set_index(
            self.groups + [self.name])

        def max_index_for_row(row):
            groups = row[self.groups]
            value = row[self.name]

            xsection = indices.xs(groups, level=self.groups)
            max_index = xsection.get_value(value, 'index')

            if isinstance(max_index, Series):
                max_index = max_index.max()

            return max_index

        groupby_max = self._groupby().max().reset_index()
        column = groupby_max.apply(max_index_for_row, axis=1).apply(int)
        column.name = self.name

        return DataFrame(column).join(groupby_max[self.groups])


class NewestAggregation(Aggregation):
    """For the newest index column get the value column."""

    formula_name = 'newest'

    index_column = 0
    value_column = 1

    def agg(self):
        self.columns[self.index_column].name = 'index'
        idframe = DataFrame(self.columns[self.value_column]).join(
            self.columns[self.index_column]).dropna().reset_index()
        idx = idframe['index'].argmax()
        result = idframe[self.name].get_value(idx)

        return self._value_to_dframe(result)

    def group(self):
        argmax_agg = ArgMaxAggregation(self.name, self.groups, self.dframe)
        argmax_df = argmax_agg.eval(self.columns)
        indices = argmax_df.pop(self.name)

        newest_col = self.columns[self.value_column][indices]
        newest_col.index = argmax_df.index

        return argmax_df.join(newest_col)


class MeanAggregation(MultiColumnAggregation):
    """Calculate the arithmetic mean.

    Written as ``mean(FORMULA)``. Where `FORMULA` is a valid formula.

    Because mean is irreducible this inherits from `MultiColumnAggregation` to
    use its reduce generic implementation.
    """

    formula_name = 'mean'

    def group(self):
        return super(self.__class__, self).group(
            [self.column, Series([1] * len(self.column))])

    def agg(self):
        dframe = DataFrame(index=[0])

        columns = [
            Series([col]) for col in [self.column.sum(), len(self.column)]]

        dframe = self._build_dframe(dframe, columns)
        dframe = DataFrame([dframe.sum().to_dict()])

        return self._add_calculated_column(dframe)


class MedianAggregation(Aggregation):
    """Calculate the median. Written as ``median(FORMULA)``.

    Where `FORMULA` is a valid formula.
    """

    formula_name = 'median'


class MinAggregation(Aggregation):
    """Calculate the minimum.

    Written as ``min(FORMULA)``. Where `FORMULA` is a valid formula.
    """

    formula_name = 'min'


class SumAggregation(Aggregation):
    """Calculate the sum.

    Written as ``sum(FORMULA)``. Where `FORMULA` is a valid formula.
    """

    formula_name = 'sum'

    def reduce(self, dframe, columns):
        self.columns = columns
        self.column = columns[0]
        dframe[self.name] += self.agg()[self.name]

        return dframe


class RatioAggregation(MultiColumnAggregation):
    """Calculate the ratio.

    Columns with N/A for either the numerator or denominator are ignored.  This
    will store associated numerator and denominator columns.  Written as
    ``ratio(NUMERATOR, DENOMINATOR)``. Where `NUMERATOR` and `DENOMINATOR` are
    both valid formulas.
    """

    formula_name = 'ratio'

    def group(self):
        return super(self.__class__, self).group(self.columns)

    def agg(self):
        dframe = DataFrame(index=self.column.index)

        dframe = self._build_dframe(dframe, self.columns)
        column_names = [self._name_for_idx(i) for i in xrange(0, 2)]
        dframe = dframe.dropna(subset=column_names)

        dframe = DataFrame([dframe.sum().to_dict()])

        return self._add_calculated_column(dframe)


class CountAggregation(Aggregation):
    """Calculate the count of rows fulfilling the criteria in the formula.

    N/A values are ignored unless there is no arguments to the
    function, in which case is simply returns the number of rows
    in the dataset.
    Written as
    ``count(CRITERIA)``. Where `CRITERIA` is an optional boolean expression
    that signifies which rows are to be counted.
    """

    formula_name = 'count'

    def group(self):
        if self.column is not None:
            joined = self.dframe[self.groups].join(
                self.column)
            joined = joined[self.column]
            groupby = joined.groupby(self.groups, as_index=False)
            result = groupby.agg(
                self.formula_name)[self.column.name].reset_index()
        else:
            result = self.dframe[self.groups]
            result = result.groupby(self.groups).apply(lambda x: len(x)).\
                reset_index().rename(columns={0: self.name})

        return result

    def agg(self):
        if self.column is not None:
            self.column = self.column[self.column]
            result = float(self.column.__getattribute__(self.formula_name)())
        else:
            result = len(self.dframe)

        return self._value_to_dframe(result)


# dict of formula names to aggregation classes
AGGREGATIONS = {
    cls.formula_name: cls for cls in
    Aggregation.__subclasses__() + MultiColumnAggregation.__subclasses__()
    if cls.formula_name
}
