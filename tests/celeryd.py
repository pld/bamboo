import os
from signal import SIGTERM
import sys
import time
from subprocess import Popen

from daemon import Daemon


class CeleryDaemon(Daemon):
    """
    Daemon wrapper for celeryd
    """

    celeryd_pf = '/tmp/celeryd.pid'

    def __init__(self, celeryconfig, *args, **kwargs):
        self.celery = None
        self.celeryconfig = celeryconfig
        Daemon.__init__(self, *args, **kwargs)

    def run(self):
        args = [
            'celeryd',
            '--config=%s' % self.celeryconfig',
            '--loglevel=CRITICAL',
            '--pidfile=%s' % self.celeryd_pf,
        ]
        Popen(args)
        while True:
            time.sleep(1)

    def stop(self):
        try:
            pf = file(self.celeryd_pf, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            os.kill(pid, SIGTERM)
        Daemon.stop(self)


if __name__ == '__main__':
    logfile = '/tmp/celerydd.log'
	celeryd = CeleryDaemon(
        'config.celeryconfig',
        '/tmp/celerydd.pid',
        stdout=logfile,
        stderr=logfile
    )
	if len(sys.argv) == 2:
		if 'start' == sys.argv[1]:
			celeryd.start()
		elif 'stop' == sys.argv[1]:
			celeryd.stop()
		elif 'restart' == sys.argv[1]:
			celeryd.restart()
		else:
			print "Unknown command"
			sys.exit(2)
		sys.exit(0)
	else:
		print "usage: %s start|stop|restart" % sys.argv[0]
		sys.exit(2)
