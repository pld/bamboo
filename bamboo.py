import cherrypy
from root import Root

conf = {
    'global': {
        'server.socket_host': '0.0.0.0',
        'server.socket_port': 8080,
    },
}

root = Root()

cherrypy.quickstart(Root(), '/', conf)
