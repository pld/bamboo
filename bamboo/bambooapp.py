#!/usr/bin/env python

import sys
sys.stdout = sys.stderr
sys.path.append('/var/www/bamboo/current')
sys.path.append('/var/www/bamboo/current/bamboo')

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

cherrypy.config.update({'environment': 'embedded'})

application = cherrypy.Application(root, script_name='/',
                                   config='config/prod.conf')
