import cherrypy
from controllers.calculate import Calculate
from controllers.collections import Collections
from controllers.root import Root

root = Root()
root.calculate = Calculate()
root.collections = Collections()

cherrypy.quickstart(root, config='config/prod.conf')
