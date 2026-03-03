# Step 1 - Create the log group
aws logs create-log-group ^
  --log-group-name /aws/kinesis-analytics/DB-testing-app ^
  --region eu-west-1

# Step 2 - Create log stream
aws logs create-log-stream ^
  --log-group-name /aws/kinesis-analytics/DB-testing-app ^
  --log-stream-name flink-job-output ^
  --region eu-west-1

# Step 3 - Get log stream ARN (note it down)
aws logs describe-log-streams ^
  --log-group-name /aws/kinesis-analytics/DB-testing-app ^
  --region eu-west-1

# Step 4 - Add logging to your Flink app
aws kinesisanalyticsv2 add-application-cloud-watch-logging-option ^
  --application-name DB-testing-app ^
  --cloud-watch-logging-option LogStreamARN="arn:aws:logs:eu-west-1:739275465799:log-group:/aws/kinesis-analytics/DB-testing-app:log-stream:flink-job-output" ^
  --region eu-west-1
