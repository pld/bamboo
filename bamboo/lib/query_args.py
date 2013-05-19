from dateutil import parser
from time import mktime

from bamboo.lib.utils import combine_dicts, replace_keys


def parse_order_by(order_by):
    if order_by:
        if order_by[0] in ('-', '+'):
            sort_dir, field = -1 if order_by[0] == '-' else 1, order_by[1:]
        else:
            sort_dir, field = 1, order_by
        order_by = [(field, sort_dir)]

    return order_by


def parse_dates_from_query(query, dataset):
    if query and dataset:
        for col in dataset.schema.datetimes(query.keys()):
            query[col] = maybe_parse_date(query[col])

    return query or {}


def maybe_parse_date(o):
    if isinstance(o, dict):
        return {k: maybe_parse_date(v) for k, v in o.items()}
    elif isinstance(o, list):
        return [maybe_parse_date(e) for e in o]
    elif isinstance(o, basestring):
        return mktime(parser.parse(o).timetuple())
    else:
        return o


class QueryArgs(object):
    def __init__(self, query=None, select=None, distinct=None, limit=0,
                 order_by=None, dataset=None):
        """A holder for query arguments.

        :param query: An optional query.
        :param select: An optional select to limit the fields in the dframe.
        :param distinct: Return distinct entries for this field.
        :param limit: Limit on the number of rows in the returned dframe.
        :param order_by: Sort resulting rows according to a column value and
            sign indicating ascending or descending.

        Example of `order_by`:

          - ``order_by='mycolumn'``
          - ``order_by='-mycolumn'``
        """
        self.query = parse_dates_from_query(query, dataset)
        self.select = select
        self.distinct = distinct
        self.limit = limit
        self.order_by = parse_order_by(order_by)

    def encode(self, encoding, query):
        """Encode query, order_by, and select given an encoding.

        The query will be combined with the existing query.

        :param encoding: A dict to encode the QueryArgs fields with.
        :param query: An additional dict to combine with the existing query.
        """
        self.query = replace_keys(combine_dicts(self.query, query), encoding)
        self.order_by = self.order_by and replace_keys(dict(self.order_by),
                                                       encoding).items()
        self.select = self.select and replace_keys(self.select, encoding)

    def __nonzero__(self):
        return bool(self.query or self.select or self.distinct or self.limit
                    or self.order_by)
