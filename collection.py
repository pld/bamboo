import json

import cherrypy

from db import db

SOURCE = '_source'

class Collection(object):

    def __init__(self):
        pass

    exposed = True

    def GET(self, d=None):
        if d:
            r = [x for x in db()[d].find()]
            return json.dumps(r)
        return "Ohai World!"

    def POST(self, d=None):
        if d:
            if 'http://' in d or 'https://' in d:
                f = urllib2.urlopen(d)
                df = read_csv(f, na_values=['n/a'])
                j = df_to_json(df)
                # add metadata to file
                for i, e in enumerate(j):
                    e[SOURCE] = d
                    j[i] = e
                # store in db, create collection for file
                # insert data into collection
                db()[d].insert(j)
                return json.dumps(j)
