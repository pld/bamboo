#!/bin/sh

FILE=""
PROFILE=""

while getopts "p:f:" opt
do
  case "$opt" in
    f) FILE=$OPTARG ;;
    p) PROFILE=$OPTARG ;;
  esac
done

if [ -z "$PROFILE" ]
then
  nosetests --with-progressive --with-cov --cov-report term-missing ../bamboo
else
  nosetests --with-profile $FILE --profile-restrict $PROFILE
fi
