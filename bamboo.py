import cherrypy

from controllers.calculate import Calculate
from controllers.observations import Observations
from controllers.root import Root

root = Root()
root.calculate = Calculate()
root.observations = Observations()

cherrypy.quickstart(root, config='config/prod.conf')
