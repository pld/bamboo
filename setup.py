from distutils.core import setup

setup(
    name='bamboo-data',
    version='0.5.4',
    author='Modi Research Group',
    author_email='info@modilabs.org',
    packages=['bamboo',
              'bamboo.config',
              'bamboo.controllers',
              'bamboo.core',
              'bamboo.lib',
              'bamboo.models',
              'bamboo.tests.core',
              'bamboo.tests.controllers',
              'bamboo.tests.lib',
              'bamboo.tests.models',
              ],
    package_data={'bamboo.tests': ['tests/fixtures/*.csv',
                                   'tests/fixtures/*.json',
                                   ],
                  },
    scripts=['scripts/bamboo.sh',
             'scripts/bamboo_uwsgi.sh',
             'scripts/commands.sh',
             'scripts/install.sh',
             'scripts/run_server.py',
             'scripts/test.sh',
             'scripts/timeit.sh',
             'celeryd/celeryd',
             'celeryd/celeryd-centos'],
    url='http://bamboo.io',
    description='Dynamic data analysis over the web. The logic to your data '
                'dashboards.',
    long_description=open('README.rst', 'rt').read(),
    install_requires=[
        # celery requires python-dateutil>=1.5,<2.0
        'python-dateutil==1.5',

        # for pandas
        'numpy',
        'pandas',
        'scipy',

        # for celery
        'kombu',
        'celery',
        'pymongo',

        'cherrypy',
        'pyparsing',
        'simplejson',
        'Routes'
    ],
)
