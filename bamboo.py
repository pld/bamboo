import cherrypy

from controllers.calculate import Calculate
from controllers.datasets import Datasets
from controllers.root import Root

root = Root()
root.calculate = Calculate()
root.datasets = Datasets()

cherrypy.quickstart(root, config='config/prod.conf')
