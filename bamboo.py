import cherrypy

from controllers.datasets import Datasets
from controllers.root import Root

root = Root()
root.datasets = Datasets()

cherrypy.tree.mount(root, '/')

if __name__ == '__main__': # pragma: no cover
    cherrypy.quickstart(root, config='config/prod.conf')
