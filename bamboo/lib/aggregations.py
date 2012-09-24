class Aggregation(object):

    def group_aggregation(self, groupby):
        return groupby.agg(self.name)

    def column_aggregation(self, column):
        return float(column.__getattribute__(self.name)())


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
