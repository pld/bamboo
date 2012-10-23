from pandas import DataFrame, Series


class Aggregation(object):
    """
    Abstract class for all aggregations.
    """

    def group(self, dframe, groups, columns):
        """
        For when aggregation is called with a group parameter.
        """
        column = columns[0]
        groupby = dframe[groups].join(
            column).groupby(groups, as_index=False)
        return groupby.agg(self.name)

    def column(self, columns, name):
        """
        For when aggregation is called without a group parameter.
        """
        column = columns[0]
        result = float(column.__getattribute__(self.name)())
        return DataFrame({name: Series([result])})

    def _reduce(self, dframe, columns, name):
        return dframe


class MultiColumnAggregation(Aggregation):
    """
    Interface for aggregations that create multiple columns.
    """
    def _reduce(self, dframe, columns, name):
        new_dframe = self.column(columns, name)
        for column in new_dframe.columns:
            dframe[column] += new_dframe[column]
        dframe[name] = self._agg_dframe(dframe, name)
        return dframe

    def _name_for_idx(self, name, idx):
        return '%s_%s' % (name, {
            0: 'numerator',
            1: 'denominator',
        }[idx])

    def _build_dframe(self, dframe, name, columns):
        for idx, column in enumerate(columns):
            column.name = self._name_for_idx(name, idx)
            dframe = dframe.join(column)

        return dframe

    def _agg_dframe(self, dframe, name):
        return dframe[self._name_for_idx(name, 0)].apply(float) /\
            dframe[self._name_for_idx(name, 1)]


class MaxAggregation(Aggregation):
    """
    Calculate the maximum.
    """

    name = 'max'


class MeanAggregation(MultiColumnAggregation):
    """
    Calculate the arithmetic mean.
    """

    name = 'mean'

    def column(self, columns, name):
        column = columns[0]
        dframe = DataFrame(index=[0])

        columns = [Series([col]) for col in [column.sum(), len(column)]]
        dframe = self._build_dframe(dframe, name, columns)

        dframe = DataFrame([dframe.sum().to_dict()])
        column = self._agg_dframe(dframe, name)
        column.name = name
        return dframe.join(column)


class MedianAggregation(Aggregation):
    """
    Calculate the median.
    """

    name = 'median'


class MinAggregation(Aggregation):
    """
    Calculate the minimum.
    """

    name = 'min'


class SumAggregation(Aggregation):
    """
    Calculate the sum.
    """

    name = 'sum'

    def _reduce(self, dframe, columns, name):
        dframe[name] += self.column(columns, name)[name]
        return dframe


class RatioAggregation(MultiColumnAggregation):
    """
    Calculate the ratio. Columns with N/A for either the numerator or
    denominator are ignored.  This will store associated numerator and
    denominator columns.
    """

    name = 'ratio'

    def group(self, dframe, groups, columns):
        # name of formula
        name = columns[0].name
        dframe = dframe[groups]

        dframe = self._build_dframe(dframe, name, columns)

        groupby = dframe.groupby(groups, as_index=False)
        aggregated_dframe = groupby.sum()

        new_column = self._agg_dframe(aggregated_dframe, name)

        new_column.name = name
        dframe = aggregated_dframe.join(new_column)

        # need to set index to account for dropped values
        return dframe

    def column(self, columns, name):
        dframe = DataFrame(index=columns[0].index)

        dframe = self._build_dframe(dframe, name, columns)
        column_names = [self._name_for_idx(name, i) for i in xrange(0, 2)]
        dframe = dframe.dropna(subset=column_names)

        dframe = DataFrame([dframe.sum().to_dict()])
        column = self._agg_dframe(dframe, name)
        column.name = name
        return dframe.join(column)


AGGREGATIONS = dict([
    (cls.name, cls()) for cls in
    Aggregation.__subclasses__() + MultiColumnAggregation.__subclasses__()
    if hasattr(cls, 'name')])
