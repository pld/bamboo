#!/bin/bash

# set defaults
DATAPATH=$1
: ${DATAPATH:='tests/fixtures/good_eats.csv'}
GROUPBY=$2
HOSTURI='http://localhost:8080/datasets'

cd `pwd`

START=$(date '+%s.%N')
RET=$(curl -sX POST -d "url=file://$DATAPATH" $HOSTURI)
END=$(date '+%s.%N')
POSTTIME=$(echo "$END - $START" | bc)
ID=$(echo $RET | sed 's/.*"\([0-9,a-f]\+\)".*/\1/')

echo id: $ID
echo post time: $POSTTIME

START=$(date '+%s.%N')
curl -s "$HOSTURI/$ID" > /dev/null
END=$(date '+%s.%N')
GETTIME=$(echo "$END - $START" | bc)

echo get time: $GETTIME

START=$(date '+%s.%N')
curl -s "$HOSTURI/$ID/summary" > /dev/null
END=$(date '+%s.%N')
SUMMARYTIME=$(echo "$END - $START" | bc)

echo summary time: $SUMMARYTIME

if [ -n "${GROUPBY}" ]
then
    START=$(date '+%s.%N')
    curl -s "$HOSTURI/$ID/summary?group=$GROUPBY" > /dev/null
    END=$(date '+%s.%N')
    GROUPTIME=$(echo "$END - $START" | bc)

    echo group time: $GROUPTIME
fi

echo
echo data path: $DATAPATH
echo --------------------
echo post time: $POSTTIME
echo get time: $GETTIME
echo summary time: $SUMMARYTIME
if [ -n "${GROUPBY}" ]
then
    echo group time: $GROUPTIME
fi
