import unittest

import cherrypy
from cherrypy.test.helper import CPWebCase

from decorators import requires_internet


class TestBase(CPWebCase):

    def setUp(self):
        self._load_test_data()
        #self._start_cherrypy()
        #super(TestBase, self).setUp()

    def tearDown(self):
        pass
        #self._stop_cherrypy()
        #super(TestBase, self).tearDown()

    def _start_cherrypy(self):
        cherrypy.config.update({
            'server.logToScreen': False,
            'log.screen': False,
            'environment': 'embedded',
        })
        cherrypy.engine.start()

    def _stop_cherrypy(self):
        cherrypy.engine.stop()

    @requires_internet
    def _load_test_data(self):
        pass

    def test_true(self):
        self.assertTrue(True)
