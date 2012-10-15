#!/bin/sh

PROFILE=""

while getopts "p" opt
do
  case "$opt" in
    p) PROFILE=1 ;;
  esac
done

if [ -z "$PROFILE" ]
then
  nosetests --with-progressive --with-cov --cov-report term-missing ../bamboo
else
  echo "beginning profile tests..."
  mkdir -p ../profiling
  FILEPATH=../profiling/$(date +%Y_%m_%d-%H_%M).noseprof
  nosetests ../bamboo/tests/test_profile.py --nocapture --with-profile 2>&1 |\
    grep bamboo/bamboo | grep -v test > $FILEPATH
fi
