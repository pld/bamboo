import cherrypy

from controllers.calculations import Calculations
from controllers.datasets import Datasets
from controllers.root import Root
from controllers.version import Version

root = Root()
root.calculations = Calculations()
root.datasets = Datasets()
root.version = Version()

cherrypy.tree.mount(root, '/')

if __name__ == '__main__':  # pragma: no cover
    cherrypy.quickstart(root, config='config/prod.conf')
