# var for bamboo server
HOST='http://bamboo.io'

# post a csv url to bamboo
RESPONSE=$(curl -X POST -d "url=https://opendata.go.ke/api/views/i6vz-a543/rows.csv" $HOST/datasets)

# strip out id
ID=`echo "$RESPONSE" | sed 's/.*: "\(\w*\).*/\1/'`

# output the id
echo "The dataset id is $ID"

# calculate summary statistics
curl $HOST/datasets/$ID/summary | echo
