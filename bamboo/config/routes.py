from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.controllers.root import Root
from bamboo.controllers.version import Version

def connect_routes(dispatcher):
    """
    This function takes the dispatcher and attaches the routes.
    """
    # controller instances
    root = Root()
    calculations = Calculations()
    datasets = Datasets()
    version = Version()

    # define routes
    routes = [
        ('index', 'GET', '/', root, 'index'),
    ]
    # map them into args to dispatcher
    dictify = lambda x: dict(zip(
        ['name', 'conditions', 'route', 'controller', 'action'], x))
    kwarg_map = lambda d: dict([
        (k, dict(method=v)) if k == 'conditions'
        else (k, v) for k, v in d.iteritems()])
    routes = [kwarg_map(dictify(route)) for route in routes]

    # attach them
    for route in routes:
        dispatcher.connect(**route)
