#!/usr/bin/env python

import sys
sys.stdout = sys.stderr
sys.path.append('/var/www/bamboo/current')
sys.path.append('/var/www/bamboo/current/bamboo')

import cherrypy

from bamboo.config.routes import connect_routes


# use routes dispatcher
dispatcher = cherrypy.dispatch.RoutesDispatcher()
routes_conf = {'/': {'request.dispatch': dispatcher}}
prod_conf = 'config/prod.conf'

# connect routes
connect_routes(dispatcher)

# global config
cherrypy.config.update(routes_conf)
cherrypy.config.update(prod_conf)
cherrypy.config.update({'environment': 'embedded'})

# app config
application = cherrypy.tree.mount(root=None, config=routes_conf)
application.merge(prod_conf)
