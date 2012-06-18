class Aggregations(object):

    exposed = True

    def POST(self, dataset_id, name, formula, group, query=None):
        """
        Create a new aggregation for *dataset_id* called *name* that calculates
        *formula* after grouping by *group*.  The optional *query* will restrict
        the rows the aggregation is calculated for.
        """
        pass
