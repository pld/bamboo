# var for bamboo server
HOST='http://bamboo.io'

# post a csv url to bamboo
RET=$(curl -#X POST -d "url=https://opendata.go.ke/api/views/i6vz-a543/rows.csv" $HOST/datasets)

# strip out id
ID=`echo "$RET" | sed 's/.*: "\(\w*\).*/\1/'`

# output the id
echo "The dataset id is $ID"

echo -e "\nRetrieve data"
RET=$(curl -#g $HOST/datasets/$ID?query='{"School%20Zone":"RWIKA"}')
echo $RET

echo -e "\nCalculate summary statistics"
RET=$(curl -#g $HOST/datasets/$ID/summary?query='{"School%20Zone":"RWIKA"}')
echo $RET

echo -e "\nCalculate summary statistics with a grouping (truncated)"
RET=$(curl -#g $HOST/datasets/$ID/summary?query='{"Public%20or%20Private":"PRIVATE"}'\&group=District)
echo $RET | cut -c -1000

echo -e "\nStore calculation formula"
RET=$(curl -#X POST -d "name=small_schools&formula=Acreage<10" $HOST/calculations/$ID)
echo $RET

echo -e "\nRetrieve new calculated column (truncated)"
RET=$(curl -#g $HOST/datasets/$ID?select='{"small_schools":1}')
echo $RET | cut -c -1000
