Server Setup
============

Running the server in the foreground
------------------------------------

1. start mongodb on localhost and standard port

2. start a celery worker

    .. code-block:: sh

        $ celery worker --config=bamboo.config.celeryconfig

3. start the server

    .. code-block:: sh

        $ python ./scripts/run_server.py

Running the server as a daemon
------------------------------

start mongodb on localhost and standard port

1. create a user named 'bamboo', with home directory ``/home/bamboo``
2. create a virtualenv using virtualenvwrapper called 'bamboo'
3. place the bamboo root directory in ``/var/www/bamboo/current``

start the daemon using:

.. code-block:: sh

    $ /var/www/bamboo/current/scripts/bamboo.sh start

stop the daemon using:

.. code-block:: sh

    $ /var/www/bamboo/current/scripts/bamboo.sh stop

Example Usage
-------------

On your local server
^^^^^^^^^^^^^^^^^^^^

start the bamboo server as above, then

run the example basic commands

.. code-block:: sh

    $ ./scripts/commands.sh -l

make requests to your local server

.. code-block:: sh

    $ curl -X POST -d "url=http://formhub.org/mberg/forms/good_eats/data.csv" http://localhost:8080/datasets

Global Configuration Notes
--------------------------

- Summarization on factor columns is disallowed based upon the number of
  unique entries.  This is controlled by a parameter in the
  ``core/summary`` module.
