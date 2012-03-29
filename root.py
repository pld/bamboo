import csv
import json
import urllib2

import cherrypy
from pandas import read_csv

from db import db

class Root(object):

    def index(self, d=None):
        if d:
            if 'http://' in d or 'https://' in d:
                f = urllib2.urlopen(d)
                df = read_csv(f, na_values=['n/a'])
                r = df.to_dict()
                return r
                # TODO create collection for file
                # TODO insert data into collection
        return "Ohai World!"
    index.exposed = True
