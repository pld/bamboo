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

    # define routes as tuples:
    # (name, method, route, controller, action)
    routes = [
        # root
        ('root', 'GET',
            '/', root, 'index'),
        # datasets
        ('datasets_delete', 'DELETE',
            '/datasets/:dataset_id', datasets, 'delete'),
        ('datasets_create', 'POST',
            '/datasets', datasets, 'create'),
        ('datasets_show', 'GET',
            '/datasets/:dataset_id', datasets, 'show'),
        ('datasets_info', 'GET',
            '/datasets/:dataset_id/info', datasets, 'info'),
        ('datasets_summary', 'GET',
            '/datasets/:dataset_id/summary', datasets, 'summary'),
        ('datasets_related', 'GET',
            '/datasets/:dataset_id/related', datasets, 'related'),
        ('datasets_merge', 'POST',
            '/datasets/merge', datasets, 'merge'),
        # calculations
        ('calculations_create', 'POST',
            '/calculations/:dataset_id', calculations, 'create'),
        ('calculations_create_alias', 'POST',
            '/datasets/:dataset_id/calculations', calculations, 'show'),
        ('calculations_show', 'GET',
            '/calculations/:dataset_id', calculations, 'show'),
        ('calculations_show_alias', 'GET',
            '/datasets/:dataset_id/calculations', calculations, 'show'),
        ('calculations_delete', 'DELETE',
            '/calculations/:dataset_id', calculations, 'delete'),
        ('calculations_delete_alias', 'DELETE',
            '/datasets/:dataset_id/calculations', calculations, 'delete'),
        # version
        ('version', 'GET',
            '/', version, 'index'),
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
