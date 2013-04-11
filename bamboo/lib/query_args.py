class QueryArgs(object):
    def __init__(self, query=None, select=None, distinct=None, limit=0,
                 order_by=None):
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
        self.query = query or {}
        self.select = select
        self.distinct = distinct
        self.limit = limit
        self.order_by = self.__parse_order_by(order_by)

    def __nonzero__(self):
        return bool(self.query or self.select or self.distinct or self.limit
                    or self.order_by)

    def __parse_order_by(self, order_by):
        if order_by:
            if order_by[0] in ('-', '+'):
                sort_dir, field = -1 if order_by[0] == '-' else 1, order_by[1:]
            else:
                sort_dir, field = 1, order_by
            order_by = [(field, sort_dir)]

        return order_by
