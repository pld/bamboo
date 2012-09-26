class JSONError(Exception):
    """
    For errors while parsing JSON.
    """
    pass


class MergeError(Exception):
    """
    For errors while merging datasets.
    """
    pass


class ParseError(Exception):
    """
    For errors while parsing formulas.
    """
    pass
