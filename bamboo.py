import cherrypy
from calculate import Calculate
from collection import Collection
from root import Root

root = Root()
root.calculate = Calculate()
root.collection = Collection()

cherrypy.quickstart(root, config='prod.conf') 
