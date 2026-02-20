re-requisites Check
bash# Check Python version (need 3.8 or 3.9)
python --version

# Check AWS CLI
aws --version

# Check AWS credentials are configured
aws sts get-caller-identity

PHASE 1 — Setup (One Time Only)
Step 1: Extract the ZIP
bashunzip flink-poc.zip
cd flink-poc
Step 2: Install Python dependencies
bashpip install boto3
pip install apache-flink==1.16.0
```

### Step 3: Download JARs from Nexus & place them
```
flink-poc/jars/flink-connector-kinesis-4.0.0-1.16.jar   ← from Nexus
flink-poc/jars/flink-s3-fs-hadoop-1.16.1.jar            ← from Nexus
Step 4: Configure AWS credentials
bashaws configure
# Enter:
# AWS Access Key ID     : <your key>
# AWS Secret Access Key : <your secret>
# Default region        : ap-south-1
# Output format         : json

PHASE 2 — Upload to S3
Step 5: Create your app S3 bucket
bashexport APP_BUCKET="flink-poc-app-nishikesh"
export REGION="ap-south-1"

aws s3 mb s3://$APP_BUCKET --region $REGION
Step 6: Upload JARs to S3
bashaws s3 cp jars/flink-connector-kinesis-4.0.0-1.16.jar \
    s3://$APP_BUCKET/jars/flink-connector-kinesis-4.0.0-1.16.jar

aws s3 cp jars/flink-s3-fs-hadoop-1.16.1.jar \
    s3://$APP_BUCKET/jars/flink-s3-fs-hadoop-1.16.1.jar

# Verify — should show 2 files
aws s3 ls s3://$APP_BUCKET/jars/
Step 7: Package and upload main.py as ZIP
bashcd flink-app/
zip flink-python-app.zip main.py
cd ..

aws s3 cp flink-app/flink-python-app.zip \
    s3://$APP_BUCKET/python/flink-python-app.zip

# Verify
aws s3 ls s3://$APP_BUCKET/python/

PHASE 3 — Deploy AWS Infrastructure
Step 8: Deploy CloudFormation
bashaws cloudformation deploy \
  --template-file cloudformation/flink-full-stack.yaml \
  --stack-name flink-poc-stack \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
  --region $REGION \
  --parameter-overrides \
      ProjectName=flink-poc \
      Environment=dev \
      FlinkAppS3Bucket=$APP_BUCKET \
      FlinkAppZipKey=python/flink-python-app.zip \
      KinesisConnectorJarKey=jars/flink-connector-kinesis-4.0.0-1.16.jar \
      S3FsJarKey=jars/flink-s3-fs-hadoop-1.16.1.jar \
      FlinkRuntimeVersion=FLINK-1_16
Step 9: Verify CloudFormation succeeded
bashaws cloudformation describe-stacks \
  --stack-name flink-poc-stack \
  --region $REGION \
  --query "Stacks[0].StackStatus"

# Expected output: "CREATE_COMPLETE"

PHASE 4 — Start Flink Application
Step 10: Start the Flink app
bashaws kinesisanalyticsv2 start-application \
  --application-name flink-poc-app \
  --region $REGION \
  --run-configuration '{}'
Step 11: Wait for RUNNING status
bash# Run this every 30 seconds until you see "RUNNING"
aws kinesisanalyticsv2 describe-application \
  --application-name flink-poc-app \
  --region $REGION \
  --query "ApplicationDetail.ApplicationStatus"

# Expected: "STARTING" → wait → "RUNNING"

PHASE 5 — Send Records
Step 12: First do a dry run (no AWS needed)
bashcd generator/
python record_generator.py --dry-run --count 2
You should see JSON records printed like:
json{
  "message": "19-02-2026 14:23:11.432 3eelac5-SLOT3...",
  "auth_status": "FAILED",
  "account_number": "984712345678",
  ...
}
Step 13: Send real records to Kinesis
bash# Send 1 record/sec continuously
python record_generator.py \
  --stream flink-poc-input-stream \
  --region ap-south-1 \
  --rate 1

# You should see output like:
# [SENT] account=984712345678 auth=IN ip=26.55.165.88 shard=shardId-000000000000 seq=49234...
Leave this running and open a new terminal for the next steps.

PHASE 6 — Check Logs & Output
Option A: CloudWatch Logs (real-time, recommended)
Via AWS Console:

Go to AWS Console → CloudWatch
Click Log Groups in left sidebar
Open /aws/kinesis-analytics/flink-poc-app
Click flink-app-logs
You'll see live logs streaming in

Via CLI:
bash# Get log events from the stream
aws logs get-log-events \
  --log-group-name "/aws/kinesis-analytics/flink-poc-app" \
  --log-stream-name "flink-app-logs" \
  --region $REGION \
  --limit 20

# Watch logs live (runs every 5 seconds)
watch -n 5 "aws logs get-log-events \
  --log-group-name '/aws/kinesis-analytics/flink-poc-app' \
  --log-stream-name 'flink-app-logs' \
  --region ap-south-1 \
  --limit 10 \
  --query 'events[*].message' \
  --output text"
```

**Expected log output:**
```
PROCESSED | account=984712345678 auth=IN ip=26.55.165.88 failed=False suspicious=False
PROCESSED | account=123456789012 auth=FAILED ip=45.23.11.99 failed=True suspicious=False
SUSPICIOUS ACTIVITY DETECTED | account=111222333444 auth=FAILED path=/olb/payment/neft.do
[ALERT] Suspicious transaction: account=111222333444 ip=99.88.77.66

Option B: S3 Output Files

⏳ Files appear after ~6 minutes (60 sec checkpoint + 5 min roll interval)

Via AWS Console:

Go to AWS Console → S3
Open bucket: flink-poc-output-<your-account-id>
Open folder: processed-logs/
Download any banking-logs-*.json file and open it

Via CLI:
bashexport ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# List output files
aws s3 ls s3://flink-poc-output-$ACCOUNT_ID/processed-logs/ --recursive

# Download all output files
aws s3 sync \
  s3://flink-poc-output-$ACCOUNT_ID/processed-logs/ \
  ./output-files/

# View contents (each line is one processed record)
cat output-files/banking-logs-*.json | python -m json.tool | head -60

PHASE 7 — Stop Everything
bash# Stop the generator
# Press Ctrl+C in the generator terminal

# Stop Flink app
aws kinesisanalyticsv2 stop-application \
  --application-name flink-poc-app \
  --region ap-south-1

Quick Troubleshooting
SymptomWhat to checkCloudFormation failsRun: aws cloudformation describe-stack-events --stack-name flink-poc-stackFlink stuck in STARTINGCheck CloudWatch logs immediately for Java errorsGenerator: ResourceNotFoundExceptionKinesis stream not ready — wait 2 min after CloudFormationNo logs in CloudWatch after 2 minFlink may have crashed — check application statusNo S3 files after 10 minCheck CloudWatch for S3 permission errors or JAR issuesTell me which one to use, I am having this07:13Looking at your Nexus screenshot, you have these versions available:
VersionAvailable2.0.0✅1.20.0✅1.18.1✅1.18.0✅1.17.1✅

Use This One ✅
flink-s3-fs-hadoop-1.18.1.jar
Because from your previous Nexus screenshot, the Kinesis connector you have is flink-connector-kinesis-4.0.0-1.16 — wait, actually looking back, you also had 1.15.4 available.

Here's the Decision
Both JARs must match the same Flink version. So:
If you use Flink runtime 1.18 on AWS Managed Flink:
flink-connector-kinesis-4.0.0-1.16.jar  ← from first Nexus screenshot
flink-s3-fs-hadoop-1.18.1.jar           ← from this screenshot

⚠️ There is a mismatch — Kinesis connector says 1.16, S3 jar says 1.18.1


My Recommendation
Since your Nexus has flink-s3-fs-hadoop-1.18.1, change everything to Flink 1.18 to keep versions consistent:
WhatVersion to useAWS Managed Flink RuntimeFLINK-1_18Kinesis connector JARflink-connector-kinesis-4.0.0-1.16.jar (still works on 1.18)S3 JARflink-s3-fs-hadoop-1.18.1.jar ✅PyFlink local installpip install apache-flink==1.18.0

Update These 2 Things
1. CloudFormation deploy command — change runtime:
bash--parameter-overrides \
    ...
    FlinkRuntimeVersion=FLINK-1_18 \         # ← change from FLINK-1_16
    S3FsJarKey=jars/flink-s3-fs-hadoop-1.18.1.jar   # ← update jar name
2. Local install — match PyFlink version:
bashpip install apache-flink==1.18.0
That's it — download flink-s3-fs-hadoop-1.18.1.jar from Nexus, place it in flink-poc/jars/, and you're good to go! Sonnet 4.6Claude is AI and can make mistakes. Please double-check responses.
