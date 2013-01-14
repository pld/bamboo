# var for bamboo server

HOST="http://bamboo.io"
WAIT_TIME=15

while getopts l opt
do
  case "$opt" in
    l) HOST='http://localhost:8080' ;;
  esac
done

echo -e "\nbamboo command demo\n"
echo -e "Using host $HOST...\n"

# post a csv url to bamboo
RET=$(curl -#X POST -d "url=https://opendata.go.ke/api/views/i6vz-a543/rows.csv" $HOST/datasets)

# upload a local file
#RET=$(curl -#X POST -F csv_file=@/home/modilabs/src/bamboo/tests/fixtures/kenya_secondary_schools_2007.csv $HOST/datasets)

# strip out id
ID=`echo "$RET" | sed 's/.*: "\(\w*\).*/\1/'`

# output the id
echo "The dataset id is $ID"

sleep $WAIT_TIME

echo -e "\nRetrieve info for dataset"
RET=$(curl -#g $HOST/datasets/$ID/info)
echo $RET

sleep $WAIT_TIME

echo -e "\nRetrieve data for School Zone RWIKA"
# using slug for "School Zone", which is "school_zone"
RET=$(curl -#g $HOST/datasets/$ID?query='{"school_zone":"RWIKA"}')
echo $RET

sleep $WAIT_TIME

echo -e "\nCalculate summary statistics for School Zone RWIKA"
# using slug for "School Zone", which is "school_zone"
RET=$(curl -#g $HOST/datasets/$ID/summary?query='{"school_zone":"RWIKA"}'\&select=all)
echo $RET

sleep $WAIT_TIME

echo -e "\nCalculate summary statistics with a grouping (truncated to 1000 characters)"
echo -e "Group by District showing only PRIVATE schools"
# using slug for "Public or Private", which is "public_or_private"
RET=$(curl -#g $HOST/datasets/$ID/summary?query='{"public_or_private":"PRIVATE"}'\&select=all\&group=district)
echo $RET | cut -c -1000

sleep $WAIT_TIME

echo -e "\nStore calculation named small_schools with formula acreage<10"
RET=$(curl -#X POST -d "name=small_schools&formula=acreage<10" $HOST/calculations/$ID)
echo $RET

sleep $WAIT_TIME

echo -e "\nRetrieve new calculated column small_schools and Districts (truncated to 2000 characters)"
RET=$(curl -#g $HOST/datasets/$ID?select='{"small_schools":1,"district":1}')
echo $RET | cut -c -2000

sleep $WAIT_TIME

echo -e "\nStore calculation named male_female_teacher_ratio with formula:"
echo -e "(tsc_male_teachers+local_authority_male_teachers+pta_board_of_governors_male_teacher+other_male_teachers)/(tsc_female_teachers+local_authority_female_teachers+pta_board_of_governors_female_teacher+other_female_teachers)"
echo -e "plus signs must by URI encoded for curl to process them correctly."
RET=$(curl -#X POST -d "name=male_female_teacher_ratio&formula=(tsc_male_teachers%2Blocal_authority_male_teachers%2Bpta_board_of_governors_male_teacher%2Bother_male_teachers)/(tsc_female_teachers%2Blocal_authority_female_teachers%2Bpta_board_of_governors_female_teacher%2Bother_female_teachers)'" $HOST/calculations/$ID)
echo $RET

sleep $WAIT_TIME

echo -e "\nRetrieve new calculated column male_female_teacher_ratio summary"
RET=$(curl -#g $HOST/datasets/$ID/summary?select='{"male_female_teacher_ratio":1}')
echo $RET | cut -c -2000

sleep $WAIT_TIME

echo -e "\nRetrieve new calculated column male_female_teacher_ratio summary grouped by province (truncated to 2000 characters)"
RET=$(curl -#g $HOST/datasets/$ID/summary?select='{"male_female_teacher_ratio":1}'&group=province)
echo $RET | cut -c -2000

sleep $WAIT_TIME

echo -e "\nStore aggregation sum(tsc_male_teachers)"
RET=$(curl -#X POST -d "name=sum_tsc_male_teachers&formula=sum(tsc_male_teachers)" $HOST/calculations/$ID)
echo $RET

sleep $WAIT_TIME

echo -e "\nRetrieve linked dataset IDs"
RET=$(curl -#g $HOST/datasets/$ID/aggregations)
echo $RET

sleep $WAIT_TIME

echo -e "\nRetrieve linked dataset"
LINKED_ID=`echo "$RET" | sed 's/.*: "\(\w*\).*/\1/'`
RET=$(curl -#g $HOST/datasets/$LINKED_ID)
echo $RET

sleep $WAIT_TIME

echo -e "\nStore aggregation sum(tsc_male_teachers) grouped by province"
RET=$(curl -#X POST -d "name=sum_tsc_male_teachers&formula=sum(tsc_male_teachers)&group=province" $HOST/calculations/$ID)
echo $RET

sleep $WAIT_TIME

echo -e "\nRetrieve linked dataset IDs"
RET=$(curl -#g $HOST/datasets/$ID/aggregations)
echo $RET

sleep $WAIT_TIME

echo -e "\nRetrieve linked dataset"
LINKED_ID=`echo "$RET" | sed 's/.*: "\(\w*\).*/\1/'`
RET=$(curl -#g $HOST/datasets/$LINKED_ID)
echo $RET
