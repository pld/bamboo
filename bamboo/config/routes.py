from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.controllers.root import Root
from bamboo.controllers.version import Version

# define routes as tuples:
# (name, method, route, controller, action)
ROUTES = [
    # Options for Cross Origin Resource Sharing (CORS)
    ('dataset_options', 'OPTIONS',
        '/datasets/:dataset_id', 'datasets', 'options'),
    ('calculations_options', 'OPTIONS',
        '/datasets/:dataset_id/calculations', 'calculations', 'options'),
    ('calculations_options_alias', 'OPTIONS',
        '/datasets/:dataset_id/calculations/:name', 'calculations', 'options'),
    # root
    ('root', 'GET',
        '/', 'root', 'index'),
    # datasets
    ('datasets_aggregations', 'GET',
        '/datasets/:dataset_id/aggregations', 'datasets', 'aggregations'),
    ('datasets_create', 'POST',
        '/datasets', 'datasets', 'create'),
    ('datasets_delete', 'DELETE',
        '/datasets/:dataset_id', 'datasets', 'delete'),
    ('datasets_drop_columns', 'PUT',
        '/datasets/:dataset_id/drop_columns', 'datasets', 'drop_columns'),
    ('datasets_info', 'GET',
        '/datasets/:dataset_id/info', 'datasets', 'info'),
    ('datasets_merge', 'POST',
        '/datasets/merge', 'datasets', 'merge'),
    ('datasets_join', 'POST',
        '/datasets/:dataset_id/join', 'datasets', 'join'),
    ('datasets_join_alias', 'POST',
        '/datasets/join', 'datasets', 'join'),
    ('datasets_resample', 'GET',
        '/datasets/:dataset_id/resample', 'datasets', 'resample'),
    ('datasets_rolling', 'GET',
        '/datasets/:dataset_id/rolling', 'datasets', 'rolling'),
    ('datasets_set_olap_type', 'PUT',
        '/datasets/:dataset_id/set_olap_type', 'datasets', 'set_olap_type'),
    ('datasets_show', 'GET',
        '/datasets/:dataset_id.:format', 'datasets', 'show'),
    ('datasets_show', 'GET',
        '/datasets/:dataset_id', 'datasets', 'show'),
    ('datasets_set_info', 'PUT',
        '/datasets/:dataset_id/info', 'datasets', 'set_info'),
    ('datasets_summary', 'GET',
        '/datasets/:dataset_id/summary', 'datasets', 'summary'),
    ('datasets_update', 'PUT',
        '/datasets/:dataset_id', 'datasets', 'update'),
    ('datasets_row_delete', 'DELETE', '/datasets/:dataset_id/row/:index',
        'datasets', 'row_delete'),
    ('datasets_row_show', 'GET', '/datasets/:dataset_id/row/:index',
        'datasets', 'row_show'),
    ('datasets_row_update', 'PUT', '/datasets/:dataset_id/row/:index',
        'datasets', 'row_update'),
    # calculations
    ('calculations_create', 'POST',
        '/calculations/:dataset_id', 'calculations', 'create'),
    ('calculations_create_alias', 'POST',
        '/datasets/:dataset_id/calculations', 'calculations', 'create'),
    ('calculations_delete', 'DELETE',
        '/datasets/:dataset_id/calculations', 'calculations', 'delete'),
    ('calculations_delete_alias', 'DELETE',
        '/datasets/:dataset_id/calculations/:name', 'calculations', 'delete'),
    ('calculations_show', 'GET',
        '/calculations/:dataset_id', 'calculations', 'show'),
    ('calculations_show_alias', 'GET',
        '/datasets/:dataset_id/calculations', 'calculations', 'show'),
    # version
    ('version', 'GET',
        '/version', 'version', 'index'),
]


def connect_routes(dispatcher):
    """This function takes the dispatcher and attaches the routes.

    :param dispatcher: The CherryPy dispatcher.
    """
    # controller instances map
    controllers = {
        'root': Root(),
        'calculations': Calculations(),
        'datasets': Datasets(),
        'version': Version(),
    }

    # map them into args to dispatcher
    dictify = lambda x: dict(zip(
        ['name', 'conditions', 'route', 'controller', 'action'], x))
    route_case = {
        'conditions': lambda v: dict(method=v),
        'controller': lambda v: controllers[v],
    }
    kwarg_map = lambda d: {
        k: route_case.get(k, lambda v: v)(v) for k, v in d.iteritems()
    }

    routes = [kwarg_map(dictify(route)) for route in ROUTES]

    # attach them
    for route in routes:
        dispatcher.connect(**route)
