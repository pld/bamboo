import cherrypy

from controllers.datasets import Datasets
from controllers.root import Root

root = Root()
root.datasets = Datasets()

if __name__ == '__main__':
    cherrypy.quickstart(root, config='config/prod.conf')
