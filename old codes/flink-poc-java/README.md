# Flink POC — Complete Java Codebase
## Kinesis Producer + AWS Managed Flink Consumer

```
[KinesisRecordProducer.java]  ──▶  [Kinesis Stream]  ──▶  [FlinkConsumerMain.java]  ──▶  [S3]
     (run on your laptop)                                    (runs on AWS MSF)               (CloudWatch logs auto)
```

---

## Project Structure

```
flink-poc-complete/
├── pom.xml
├── README.md
└── src/main/java/com/example/flink/
    ├── FlinkConsumerMain.java          ← Flink app entry point (deploy to AWS MSF)
    ├── RecordEnricher.java             ← MapFunction: parse + enrich + risk detection
    ├── DatePartitionBucketAssigner.java← S3 partition: year=YYYY/month=MM/day=DD/
    └── KinesisRecordProducer.java      ← Standalone producer (run on laptop / EC2)
```

---

## Step 1 — Build

```bash
mvn clean package -DskipTests
```

Output: `target/flink-poc-complete-1.0.0.jar`  (fat JAR, ~80 MB)

---

## Step 2 — Run the Producer (on your laptop)

The producer pushes synthetic banking log records to Kinesis.
AWS credentials are picked up from `~/.aws/credentials` (run `aws configure` first).

```bash
# Basic — 5 records/sec, runs forever (Ctrl+C to stop)
java -cp target/flink-poc-complete-1.0.0.jar \
     com.example.flink.KinesisRecordProducer

# Custom rate and count
java -cp target/flink-poc-complete-1.0.0.jar \
     com.example.flink.KinesisRecordProducer \
     --stream flink-poc-nishikesh-input-stream \
     --region eu-west-1 \
     --rate 10 \
     --count 500

# Batch mode (more efficient — 25 records per API call)
java -cp target/flink-poc-complete-1.0.0.jar \
     com.example.flink.KinesisRecordProducer \
     --batch --batch-size 25 --rate 50

# Dry run — print records to stdout, nothing sent to Kinesis
java -cp target/flink-poc-complete-1.0.0.jar \
     com.example.flink.KinesisRecordProducer \
     --dry-run --count 3
```

### Producer CLI Options

| Argument       | Default                            | Description                            |
|----------------|------------------------------------|----------------------------------------|
| `--stream`     | flink-poc-nishikesh-input-stream   | Kinesis stream name                    |
| `--region`     | eu-west-1                          | AWS region                             |
| `--rate`       | 5                                  | Records per second                     |
| `--count`      | 0 (infinite)                       | Stop after N records (0 = forever)     |
| `--batch`      | off                                | Use put_records batch API              |
| `--batch-size` | 25                                 | Records per batch (max 500)            |
| `--dry-run`    | off                                | Print to stdout only, skip Kinesis     |

---

## Step 3 — Upload JAR to S3

```bash
aws s3 cp target/flink-poc-complete-1.0.0.jar \
    s3://flink-poc-app-nishikesh/java/flink-java-app.jar \
    --region eu-west-1

# Verify
aws s3 ls s3://flink-poc-app-nishikesh/java/ --region eu-west-1
```

---

## Step 4 — Deploy on AWS Managed Flink Console

### Create Application

| Field            | Value                                                    |
|------------------|----------------------------------------------------------|
| Application name | `flink-poc-nishikesh`                                    |
| Runtime          | Apache Flink 1.18                                        |
| IAM role         | `arn:aws:iam::739275465799:role/svc-bsp-msf-flink-role`  |

### Application Code

| Field     | Value                          |
|-----------|--------------------------------|
| S3 bucket | `flink-poc-app-nishikesh`      |
| S3 path   | `java/flink-java-app.jar`      |

### Runtime Properties — Group: `ApplicationConfig`

| Key               | Value                                                |
|-------------------|------------------------------------------------------|
| `InputStreamName` | `flink-poc-nishikesh-input-stream`                   |
| `Region`          | `eu-west-1`                                          |
| `S3OutputPath`    | `s3://739275465799-flink-poc-output/processed-logs/` |
| `StreamPosition`  | `LATEST`                                             |

### Scaling

| Field               | Value |
|---------------------|-------|
| Parallelism         | `2`   |
| Parallelism per KPU | `1`   |

### Monitoring

| Field                | Value  |
|----------------------|--------|
| Monitoring log level | `INFO` |
| CloudWatch logs      | Auto-created by MSF |

---

## Step 5 — Verify End-to-End

### Check records arriving in Kinesis
```bash
SHARD_IT=$(aws kinesis get-shard-iterator \
  --stream-name flink-poc-nishikesh-input-stream \
  --shard-id shardId-000000000000 \
  --shard-iterator-type LATEST \
  --region eu-west-1 \
  --query 'ShardIterator' --output text)

aws kinesis get-records \
  --shard-iterator "$SHARD_IT" \
  --region eu-west-1 \
  --query 'Records[*].Data' \
  --output text | base64 --decode | python3 -m json.tool
```

### Check Flink CloudWatch logs
```
Log group : /aws/kinesis-analytics/flink-poc-nishikesh
Filter    : processingTimestamp
```

### Check S3 output
```bash
aws s3 ls s3://739275465799-flink-poc-output/processed-logs/ \
  --recursive --region eu-west-1
```

---

## Record Structure

### Input (produced by KinesisRecordProducer)

```json
{
  "message":             "18-11-2025 02:06:19.243 3eelac5bfaf0-SLOT2  ROLB-AWS-ACTIVITY...",
  "@timestamp":          "2025-11-18T02:06:19.244Z",
  "messageType":         "DATA_MESSAGE",
  "subscriptionFilters": ["Send-logs-to-Cloud-Watch-Logs-destination"],
  "id":                  "39325838323762907076905602175487383946170451023564046360",
  "logstash":            { "hostname": "ldwdsr00t3tuhi9" },
  "jvmId":               "195734",
  "Timestamp":           "18-11-2025 02:06:19.243",
  "ifetype":             "app",
  "time_interval":       "18-11-2025 02:06:19.243",
  "event_date":          "2025-11-18",
  "ErrorMsgId":          1101,
  "error_detail":        "Invalid credentials supplied",
  "event_type":          "ROLB-AWS-ACTIVITY",
  "auth_status":         "FAILED",
  "user_id":             "999999999999",
  "ip_address":          "26.55.165.88",
  "country":             "IN",
  "app_url":             "/olb/authlogin/loginAppContainer.do",
  "rolb_id":             "ROLB-75381217301837-7115361",
  "ibf_token":           "40bb584a-46e9-475e-80be-9b0813ac5ed9",
  "session_ids":         ["-1970162495149732183", "1763431579243"],
  "is_failed_auth":      true,
  "failed_attempts":     3,
  "producer_meta":       { "sent_at": "...", "record_no": 42, "source": "kinesis-java-producer" }
}
```

### Output (enriched by RecordEnricher, written to S3)

All original fields are kept. These are added by Flink:

| Field                 | Example value             | Description                         |
|-----------------------|---------------------------|-------------------------------------|
| `processingTimestamp` | `2025-11-18T02:06:20Z`    | When Flink processed the record     |
| `processingHost`      | `aws-managed-flink`       | Fixed identifier                    |
| `appVersion`          | `1.0.0`                   | App version tag                     |
| `isFailedAuth`        | `true`                    | Auth failed or failedAttempts >= 3  |
| `isSuspicious`        | `true`                    | Any risk rule triggered             |
| `suspiciousReasons`   | `REPEATED_FAILED_AUTH:3`  | Semicolon-separated rule list       |
| `riskScore`           | `55`                      | Numeric risk score                  |
| `riskLevel`           | `HIGH`                    | NONE/LOW/MEDIUM/HIGH/CRITICAL       |

---

## IAM Role Permissions Required

Attach to `svc-bsp-msf-flink-role`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:GetRecords", "kinesis:GetShardIterator",
        "kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
        "kinesis:ListShards", "kinesis:ListStreams",
        "kinesis:SubscribeToShard", "kinesis:RegisterStreamConsumer",
        "kinesis:DeregisterStreamConsumer", "kinesis:DescribeStreamConsumer"
      ],
      "Resource": "arn:aws:kinesis:eu-west-1:739275465799:stream/flink-poc-nishikesh-input-stream"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject","s3:GetObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts"],
      "Resource": [
        "arn:aws:s3:::739275465799-flink-poc-output/processed-logs/*",
        "arn:aws:s3:::flink-poc-app-nishikesh/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::739275465799-flink-poc-output","arn:aws:s3:::flink-poc-app-nishikesh"]
    },
    {
      "Effect": "Allow",
      "Action": ["logs:PutLogEvents","logs:CreateLogGroup","logs:CreateLogStream",
                 "logs:DescribeLogGroups","logs:DescribeLogStreams"],
      "Resource": "arn:aws:logs:eu-west-1:739275465799:log-group:*"
    },
    {
      "Effect": "Allow",
      "Action": ["cloudwatch:PutMetricData"],
      "Resource": "*"
    }
  ]
}
```

For the **producer running on your laptop**, your IAM user/role needs:
```json
{
  "Effect": "Allow",
  "Action": ["kinesis:PutRecord","kinesis:PutRecords","kinesis:DescribeStreamSummary"],
  "Resource": "arn:aws:kinesis:eu-west-1:739275465799:stream/flink-poc-nishikesh-input-stream"
}
```
