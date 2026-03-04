# jars/ — Place Your JAR Files Here

Download these two JARs from your client's Nexus Repository Manager
and place them in this folder before local testing.

## Required JARs

| File | Version | Download From |
|---|---|---|
| `flink-connector-kinesis-4.0.0-1.16.jar` | 4.0.0-1.16 | Client Nexus → search: flink-connector-kinesis |
| `flink-s3-fs-hadoop-1.16.1.jar` | 1.16.1 | Client Nexus → search: flink-s3-fs-hadoop |

## After Downloading

1. Place files in this folder:
   ```
   flink-poc/jars/flink-connector-kinesis-4.0.0-1.16.jar
   flink-poc/jars/flink-s3-fs-hadoop-1.16.1.jar
   ```

2. Upload to S3 (for AWS deployment):
   ```bash
   aws s3 cp flink-connector-kinesis-4.0.0-1.16.jar s3://your-bucket/jars/flink-connector-kinesis-4.0.0-1.16.jar
   aws s3 cp flink-s3-fs-hadoop-1.16.1.jar s3://your-bucket/jars/flink-s3-fs-hadoop-1.16.1.jar
   ```

## Note
JARs are NOT included in this ZIP because they must be downloaded
from your client's internal Nexus repository.
