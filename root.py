import csv
import json
import urllib2

import cherrypy

from db import db

class Root(object):

    def index(self, doc=None):
        if doc:
            if 'http://' in doc or 'https://' in doc:
                f = urllib2.urlopen(doc)
                d = self.csv_to_dict(f)
                return json.dumps(d)
                # create collection for file
                # insert data into collection
        return "Hello World!"
    index.exposed = True


    def csv_to_dict(self, f):
        reader = csv.reader(f)
        # assume first row is column headers
        labels = reader.next()
        data = []
        return reduce(lambda ary, row:
            ary + [reduce(lambda d, y: dict(d.items() + [(labels[y[0]], y[1])]),
                    enumerate(row), {})], reader, [])
