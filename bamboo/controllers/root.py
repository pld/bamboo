import cherrypy


class Root(object):

    def index(self):
        raise cherrypy.HTTPRedirect('/docs/index.html')

    def options(self):
        cherrypy.response.headers['Access-Control-Allow-Origin'] = '*'
        cherrypy.response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
        cherrypy.response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Accept'
        cherrypy.response.headers['Access-Control-Max-Age'] = '10'
        cherrypy.response.headers['Content-Length'] = 0
        cherrypy.response.status = 204
        return ''
