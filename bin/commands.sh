# var for bamboo server

HOST="http://bamboo.io"

while getopts l opt
do
  case "$opt" in
    l) HOST='http://localhost:8080' ;;
  esac
done

echo -e "\nBamboo command demo\n"
echo -e "Using host $HOST...\n"

# post a csv url to bamboo
RET=$(curl -#X POST -d "url=https://opendata.go.ke/api/views/i6vz-a543/rows.csv" $HOST/datasets)

# strip out id
ID=`echo "$RET" | sed 's/.*: "\(\w*\).*/\1/'`

# output the id
echo "The dataset id is $ID"

echo -e "\nRetrieve info for dataset"
RET=$(curl -#g $HOST/datasets/$ID/info)
echo $RET

echo -e "\nRetrieve data for School Zone RWIKA"
RET=$(curl -#g $HOST/datasets/$ID?query='{"School%20Zone":"RWIKA"}')
echo $RET

echo -e "\nCalculate summary statistics for School Zone RWIKA"
RET=$(curl -#g $HOST/datasets/$ID/summary?query='{"School%20Zone":"RWIKA"}')
echo $RET

echo -e "\nCalculate summary statistics with a grouping (truncated)"
RET=$(curl -#g $HOST/datasets/$ID/summary?query='{"Public%20or%20Private":"PRIVATE"}'\&group=District)
echo $RET | cut -c -1000

echo -e "\nStore calculation named small_schools with formula Acreage<10"
RET=$(curl -#X POST -d "name=small_schools&formula=Acreage<10" $HOST/calculations/$ID)
echo $RET

echo -e "\nRetrieve new calculated column small_schools and Districts (truncated)"
RET=$(curl -#g $HOST/datasets/$ID?select='{"small_schools":1,"District":1}')
echo $RET | cut -c -1000
