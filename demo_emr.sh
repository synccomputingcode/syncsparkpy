# Run the prediction passing in the Spark event log and cluster config.
# Save the prediction Id.
prediction_id=`sync-cli predictions create aws-emr -e demo/emr/application_1678162862227_0001 -c demo/emr/emr-config.json | sed 's/Prediction ID: //'`
echo "Prediction Id: $prediction_id"

# Wait 10 seconds for the prediction run to complete.
sleep 10

# Get the status for the predition run.
status=`sync-cli predictions status $prediction_id`
echo "Prediction completed with status: $status"

# If success, save results to a file.
if [ "$status" = "SUCCESS" ]
then
    sync-cli predictions get $prediction_id > demo/emr/result.json
    echo "Result written to demo/emr/result.json"
fi
