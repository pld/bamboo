import cherrypy
import urllib2
from db import db

class Root(object):

    def index(self, doc=None):
        if doc:
            if 'http://' in doc or 'https://' in doc:
                f = urllib2.urlopen(doc)
                return f
        return "Hello World!"
    index.exposed = True
