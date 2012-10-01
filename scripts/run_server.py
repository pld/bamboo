#!/usr/bin/env python

import os
import sys
sys.path.append(os.getcwd())

import cherrypy

from bamboo.controllers.calculations import Calculations
from bamboo.controllers.datasets import Datasets
from bamboo.controllers.root import Root
from bamboo.controllers.version import Version

root = Root()
root.calculations = Calculations()
root.datasets = Datasets()
root.version = Version()

cherrypy.tree.mount(root, '/')

if __name__ == '__main__':  # pragma: no cover
    cherrypy.quickstart(root, config='bamboo/config/local.conf')
