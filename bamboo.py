import cherrypy
from calculate import Calculate
from collection import Collection
from root import Root

conf = {
    'global': {
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 8080,
    },
    '/': {
        'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
    },
}

root = Root()
root.calculate = Calculate()
root.collection = Collection()

cherrypy.quickstart(root, '/', conf)
