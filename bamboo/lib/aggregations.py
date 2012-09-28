from pandas import DataFrame, Series


class Aggregation(object):
    def group_aggregation(self, dframe, groups, columns):
        column = columns[0]
        groupby = dframe[groups].join(
            column).groupby(groups, as_index=False)
        return groupby.agg(self.name)

    def column_aggregation(self, columns, name):
        column = columns[0]
        result = float(column.__getattribute__(self.name)())
        return DataFrame({name: Series([result])})


class MaxAggregation(Aggregation):

    name = 'max'


class MeanAggregation(Aggregation):

    name = 'mean'


class MedianAggregation(Aggregation):

    name = 'median'


class MinAggregation(Aggregation):

    name = 'min'


class SumAggregation(Aggregation):

    name = 'sum'


class RatioAggregation(Aggregation):

    name = 'ratio'

    def group_aggregation(self, dframe, groups, columns):
        # name of formula
        name = columns[0].name
        dframe = dframe[groups]

        column_names, dframe = self._build_dframe(dframe, name, columns)

        groupby = dframe.groupby(groups, as_index=False)
        aggregated_dframe = groupby.sum()

        new_column = self._agg_dframe(aggregated_dframe, column_names)

        new_column.name = name
        dframe = aggregated_dframe.join(new_column)

        # need to set index to account for dropped values
        return dframe

    def column_aggregation(self, columns, name):
        dframe = DataFrame(index=columns[0].index)

        column_names, dframe = self._build_dframe(dframe, name, columns)

        dframe = DataFrame([dframe.sum().to_dict()])
        column = self._agg_dframe(dframe, column_names)
        column.name = name
        return dframe.join(column)

    def _agg_dframe(self, dframe, column_names):
        return dframe[column_names[0]].apply(float) / dframe[column_names[1]]

    def _build_dframe(self, dframe, name, columns):
        column_names = []

        for idx, column in enumerate(columns):
            column.name = self._name_for_idx(name, idx)
            column_names.append(column.name)
            dframe = dframe.join(column)

        dframe = dframe.dropna(subset=column_names)

        return column_names, dframe

    def _name_for_idx(self, name, idx):
        return '%s_%s' % (name, {
            0: 'numerator',
            1: 'denominator',
        }[idx])


AGGREGATIONS = dict([(cls.name, cls()) for cls in
                     Aggregation.__subclasses__()])
