# Flink POC — Step-by-Step Deployment Guide
### Flink 1.16 | PyFlink | Kinesis → S3 + CloudWatch

---

## Architecture

```
Python Generator
(record_generator.py)
       │
       │  push fake IBF-AUTHLO records
       ▼
Kinesis Stream  ←──────────────────── flink-poc-input-stream
       │
       │  Flink reads from here
       ▼
AWS Managed Flink (Flink 1.16)
(main.py)
       │
       ├──► S3 Bucket           →  processed-logs/banking-logs-*.json
       │
       └──► CloudWatch Logs     →  /aws/kinesis-analytics/flink-poc-app
```

---

## Project Structure

```
flink-poc/
├── generator/
│   └── record_generator.py      ← Run this to push fake records to Kinesis
│
├── flink-app/
│   └── main.py                  ← PyFlink app — zip this and upload to S3
│
├── cloudformation/
│   └── flink-full-stack.yaml    ← Deploy this to create all AWS resources
│
├── jars/                        ← Download JARs here (Step 1)
│   ├── flink-connector-kinesis-4.0.0-1.16.jar   ← from client Nexus
│   └── flink-s3-fs-hadoop-1.16.1.jar            ← from client Nexus
│
└── README.md
```

---

## STEP 1 — Download JARs from Client Nexus

Go to your client's **Nexus Repository Manager** and download:

| JAR File | Version | Purpose |
|---|---|---|
| `flink-connector-kinesis` | `4.0.0-1.16` | Read/write Kinesis streams |
| `flink-s3-fs-hadoop` | `1.16.1` | Write output to S3 |

Save both files into the `flink-poc/jars/` folder:
```
flink-poc/jars/flink-connector-kinesis-4.0.0-1.16.jar
flink-poc/jars/flink-s3-fs-hadoop-1.16.1.jar
```

---

## STEP 2 — Create S3 Bucket for App Code

```bash
export APP_BUCKET="flink-poc-app-nishikesh"   # change to your bucket name
export REGION="ap-south-1"

aws s3 mb s3://$APP_BUCKET --region $REGION
```

---

## STEP 3 — Upload JARs to S3

```bash
# Upload Kinesis connector JAR
aws s3 cp jars/flink-connector-kinesis-4.0.0-1.16.jar \
    s3://$APP_BUCKET/jars/flink-connector-kinesis-4.0.0-1.16.jar

# Upload S3 filesystem JAR
aws s3 cp jars/flink-s3-fs-hadoop-1.16.1.jar \
    s3://$APP_BUCKET/jars/flink-s3-fs-hadoop-1.16.1.jar

# Verify
aws s3 ls s3://$APP_BUCKET/jars/
```

---

## STEP 4 — Package PyFlink App as ZIP

AWS Managed Flink requires `main.py` as a ZIP file:

```bash
cd flink-app/
zip flink-python-app.zip main.py
cd ..

# Upload to S3
aws s3 cp flink-app/flink-python-app.zip \
    s3://$APP_BUCKET/python/flink-python-app.zip

# Verify
aws s3 ls s3://$APP_BUCKET/python/
```

---

## STEP 5 — Deploy CloudFormation Stack

```bash
aws cloudformation deploy \
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
```

### What gets created:
- IAM Role (`svc-flink-poc-role`) with PermissionsBoundary from client template
- Kinesis stream (`flink-poc-input-stream`)
- S3 output bucket (`flink-poc-output-<account-id>`)
- CloudWatch log group (`/aws/kinesis-analytics/flink-poc-app`)
- Managed Flink application (`flink-poc-app`)

---

## STEP 6 — Start the Flink Application

```bash
# Start the app
aws kinesisanalyticsv2 start-application \
  --application-name flink-poc-app \
  --region $REGION \
  --run-configuration '{}'

# Check status every 30 seconds until you see RUNNING
aws kinesisanalyticsv2 describe-application \
  --application-name flink-poc-app \
  --region $REGION \
  --query "ApplicationDetail.ApplicationStatus"
```

Wait for status: `"RUNNING"` (takes ~2 minutes)

---

## STEP 7 — Install Generator & Configure AWS

```bash
cd generator/
pip install boto3

# Make sure AWS CLI is configured
aws configure
# Access Key ID     : <from your AWS account>
# Secret Access Key : <from your AWS account>
# Default region    : ap-south-1
# Output format     : json
```

---

## STEP 8 — Run the Record Generator

```bash
cd generator/

# Dry run first — preview records without sending (no AWS needed)
python record_generator.py --dry-run --count 2

# Send 1 record/sec continuously (Ctrl+C to stop)
python record_generator.py \
  --stream flink-poc-input-stream \
  --region ap-south-1 \
  --rate 1

# Send 5 records/sec
python record_generator.py --stream flink-poc-input-stream --rate 5

# Send exactly 30 records then stop
python record_generator.py --stream flink-poc-input-stream --count 30
```

---

## STEP 9 — View Results

### Option A: CloudWatch Logs (real-time)

1. AWS Console → **CloudWatch** → **Log Groups**
2. Open: `/aws/kinesis-analytics/flink-poc-app`
3. Open stream: `flink-app-logs`

You'll see lines like:
```
PROCESSED | account=984712345678 auth=IN ip=26.55.165.88 path=/olb/dashboard/home.do failed=False suspicious=False
SUSPICIOUS ACTIVITY DETECTED | account=123456789012 auth=FAILED path=/olb/payment/neft.do
[ALERT] Suspicious transaction: account=123456789012 ip=45.23.11.99 ...
```

### Option B: S3 Files

1. AWS Console → **S3** → `flink-poc-output-<your-account-id>`
2. Folder: `processed-logs/`
3. Files appear after ~6 minutes (first checkpoint + roll interval)
4. File name format: `banking-logs-<uuid>.json`

```bash
# Download and view via CLI
aws s3 ls s3://flink-poc-output-$(aws sts get-caller-identity \
  --query Account --output text)/processed-logs/ --recursive

# Download latest files
aws s3 sync \
  s3://flink-poc-output-$(aws sts get-caller-identity --query Account --output text)/processed-logs/ \
  ./output-files/
```

---

## Sample Processed Record

**Input** (from generator → Kinesis):
```json
{
  "message": "19-02-2026 14:23:11.432 3eelac5-SLOT3 ROLB-AWS-ACTIVITY...",
  "@timestamp": "2026-02-19T14:23:11.432Z",
  "auth_status": "FAILED",
  "account_number": "984712345678",
  "client_ip": "45.23.11.99",
  "url_path": "/olb/payment/neft.do",
  "ifetype": "app"
}
```

**Output** (enriched by Flink → S3):
```json
{
  "message": "...",
  "@timestamp": "2026-02-19T14:23:11.432Z",
  "auth_status": "FAILED",
  "account_number": "984712345678",
  "client_ip": "45.23.11.99",
  "url_path": "/olb/payment/neft.do",
  "processing_timestamp": "2026-02-19T14:23:11.550Z",
  "flink_subtask_id": 0,
  "is_failed_auth": true,
  "is_successful": false,
  "is_suspicious": true,
  "event_date": "2026-02-19"
}
```

---

## Stopping the Application

```bash
# Stop generator: Ctrl+C in the terminal

# Stop Flink app
aws kinesisanalyticsv2 stop-application \
  --application-name flink-poc-app \
  --region ap-south-1
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Generator: `NoCredentialsError` | Run `aws configure` |
| Generator: `ResourceNotFoundException` | Kinesis stream not created yet — check CloudFormation status |
| Flink stuck in `STARTING` | Check CloudWatch logs for Java/JAR errors |
| No files in S3 after 10 min | Check CloudWatch logs — may be a JAR path issue |
| `ClassNotFoundException` in Flink logs | JAR key in CloudFormation doesn't match S3 path — verify exact filename |
| CloudFormation `PermissionsBoundary` error | Check the boundary policy name matches exactly: `core-ServiceRolePermissionssBoundary` |
| CloudFormation `ROLLBACK` | Check IAM — role name `svc-flink-poc-role` may already exist from a previous deploy |
