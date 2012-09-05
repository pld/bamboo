#!/bin/bash

UWSGI_BIN=/home/bamboo/.virtualenvs/bamboo/bin/uwsgi
WSGI_APP=/var/www/bamboo/current/bamboo/bambooapp.py
SOCKET=localhost:8099
PROCESSES=8
ROOT_DIR=/var/www/bamboo/current/bamboo
LOG_FILE=/var/www/bamboo/log/uwsgi_bamboo.log
UID=bamboo
GID=bamboo
VENV=/home/bamboo/.virtualenvs/bamboo

${UWSGI_BIN} --touch-reload ${WSGI_APP} --socket ${SOCKET} --protocol uwsgi --processes ${PROCESSES} --enable-threads --chdir ${ROOT_DIR} --module bambooapp:application --virtualenv ${VENV} --master --pythonpath ${ROOT_DIR} --pythonpath ${ROOT_DIR}/../ --daemonize ${LOG_FILE} --uid ${UID} --gid ${GID}
