#!/bin/bash

### BEGIN INIT INFO
# Provides:          bamboo_uwsgi
# Required-Start:    mongodb networking
# Required-Stop:     mongodb networking
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: bamboo service over uWSGI
# Description:       bamboo service over uWSGI
### END INIT INFO

# Using the lsb functions to perform the operations.
. /lib/lsb/init-functions

DESC="bamboo uWSGI server"
NAME=bamboo_uwsgi
DAEMON=/home/bamboo/.virtualenvs/bamboo/bin/uwsgi
WSGI_APP=/var/www/bamboo/current/bamboo/bambooapp.py
UWSGI_SOCKET=localhost:8099
UWSGI_PROCESSES=8
ROOT_DIR=/var/www/bamboo/current/bamboo
UWSGI_LOG_FILE=/var/www/bamboo/log/uwsgi_bamboo.log
UWSGI_PID_FILE=/var/www/bamboo/shared/pids/uwsgi_bamboo.pid
UWSGI_UID=bamboo
UWSGI_GID=bamboo
VENV=/home/bamboo/.virtualenvs/bamboo
START_ARGS="--touch-reload ${WSGI_APP} --socket ${UWSGI_SOCKET} --protocol uwsgi --processes ${UWSGI_PROCESSES} --enable-threads --chdir ${ROOT_DIR} --module bambooapp:application --virtualenv ${VENV} --master --pythonpath ${ROOT_DIR} --pythonpath ${ROOT_DIR}/../ --daemonize ${UWSGI_LOG_FILE} --uid ${UWSGI_UID} --gid ${UWSGI_GID} --vacuum --pidfile2 ${UWSGI_PID_FILE}"
STOP_ARGS="--stop ${UWSGI_PID_FILE}"
RELOAD_ARGS="--reload ${UWSGI_PID_FILE}"

# overwrite default values
if [ -f /etc/default/bamboo_uwsgi ]
    then
    source /etc/default/bamboo_uwsgi
fi

# Exit if the package is not installed
[ -x "$DAEMON" ] || exit 0

do_start()
{
    $DAEMON $START_ARGS
}

do_stop()
{
    $DAEMON $STOP_ARGS
}

do_reload()
{
    $DAEMON $RELOAD_ARGS
}

case "$1" in
  start)
    [ "$VERBOSE" != no ] && echo "Starting $DESC" "$NAME"
    do_start
    case "$?" in
        0|1) [ "$VERBOSE" != no ] && echo 0 ;;
        2) [ "$VERBOSE" != no ] && echo 1 ;;
    esac
    ;;
  stop)
    [ "$VERBOSE" != no ] && echo "Stopping $DESC" "$NAME"
    do_stop
    case "$?" in
        0|1) [ "$VERBOSE" != no ] && echo 0 ;;
        2) [ "$VERBOSE" != no ] && echo 1 ;;
    esac
    ;;
  reload)
    [ "$VERBOSE" != no ] && echo "Reloading $DESC" "$NAME"
    do_reload
    case "$?" in
        0|1) [ "$VERBOSE" != no ] && echo 0 ;;
        2) [ "$VERBOSE" != no ] && echo 1 ;;
    esac
    ;;
  status)
       status_of_proc "$DAEMON" "$NAME" && exit 0 || exit $?
       ;;
  restart)
    echo "Restarting $DESC" "$NAME"
    do_stop
    case "$?" in
      0|1)
        sleep 8
        do_start
        case "$?" in
            0) echo 0 ;;
            1) echo 1 ;; # Old process is still running
            *) echo 1 ;; # Failed to start
        esac
        ;;
      *)
        # Failed to stop
        echo 1
        ;;
    esac
    ;;
  *)
    echo "Usage: $SCRIPTNAME {start|stop|status|restart|reload}" >&2
    exit 3
    ;;
esac
