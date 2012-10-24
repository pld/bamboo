#!/usr/bin/env python

import os
import sys
sys.path.append(os.getcwd())

import cherrypy

from bamboo.config.routes import connect_routes


# use routes dispatcher
dispatcher = cherrypy.dispatch.RoutesDispatcher()
routes_conf = {'/': {'request.dispatch': dispatcher}}
local_conf = 'bamboo/config/local.conf'

# connect routes
connect_routes(dispatcher)

# global config
cherrypy.config.update(routes_conf)
cherrypy.config.update(local_conf)

# app config
app = cherrypy.tree.mount(root=None, config=routes_conf)
app.merge(local_conf)


# start server
if __name__ == '__main__':  # pragma: no cover
    cherrypy.quickstart(app)
