import cherrypy

from controllers.datasets import Datasets
from controllers.root import Root

root = Root()
root.datasets = Datasets()

cherrypy.quickstart(root, config='config/prod.conf')
