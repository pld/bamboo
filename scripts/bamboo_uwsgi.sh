#!/bin/bash

UWSGI_BIN=/home/bamboo/.virtualenvs/bamboo/bin/uwsgi
WSGI_APP=/var/www/bamboo/current/bamboo/bambooapp.py
UWSGI_SOCKET=localhost:8099
UWSGI_PROCESSES=8
ROOT_DIR=/var/www/bamboo/current/bamboo
UWSGI_LOG_FILE=/var/www/bamboo/log/uwsgi_bamboo.log
UWSGI_PID_FILE=/var/www/bamboo/shared/pids/uwsgi_bamboo.pid
UWSGI_UID=bamboo
UWSGI_GID=bamboo
VENV=/home/bamboo/.virtualenvs/bamboo

${UWSGI_BIN} --touch-reload ${WSGI_APP} --socket ${UWSGI_SOCKET} --protocol uwsgi --processes ${UWSGI_PROCESSES} --enable-threads --chdir ${ROOT_DIR} --module bambooapp:application --virtualenv ${VENV} --master --pythonpath ${ROOT_DIR} --pythonpath ${ROOT_DIR}/../ --daemonize ${UWSGI_LOG_FILE} --uid ${UWSGI_UID} --gid ${UWSGI_GID} --vacuum --pidfile2 ${UWSGI_PID_FILE}
