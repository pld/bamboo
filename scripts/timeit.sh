#!/bin/bash

REMOTE_DATA[0]='https://opendata.go.ke/api/views/i6vz-a543/rows.csv'
REMOTE_DATA[1]='https://formhub.org/north_ghana/forms/_08_Water_points_train3/data.csv'

GROUP[0]='district'
GROUP[1]='district'

RD_IDX=0
HOST="http://bamboo.io"

while getopts "lr:g:" opt
do
  case "$opt" in
    l) HOST='http://localhost:8080' ;;
    r) RD_IDX=$OPTARG ;;
    g) GROUP_BY=$OPTARG ;;
  esac
done

# set defaults
DATAPATH=${REMOTE_DATA[$RD_IDX]}
if [ -z "$GROUP_BY" ]
then
  GROUP_BY=${GROUP[$RD_IDX]}
fi

HOSTURI=$HOST/datasets

echo
echo host: $HOST
echo data path: $DATAPATH

cd `pwd`

START=$(date '+%s.%N')
RET=$(curl -sX POST -d "url=$DATAPATH" $HOSTURI)
END=$(date '+%s.%N')
POSTTIME=$(echo "$END - $START" | bc)

# strip out id
ID=`echo "$RET" | sed 's/.*: "\(\w*\).*/\1/'`

echo id: $ID
echo --------------------
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

if [ -n "${GROUP_BY}" ]
then
    START=$(date '+%s.%N')
    curl -s "$HOSTURI/$ID/summary?group=$GROUP_BY" > /dev/null
    END=$(date '+%s.%N')
    GROUPTIME=$(echo "$END - $START" | bc)

    echo group time: $GROUPTIME
fi
