#!/bin/bash
echo "=== Downloading certs from S3 ==="
aws s3 cp s3://flink-poc-app-nishikesh/certs/ca-issuing-bundle.crt /tmp/ca-issuing-bundle.crt
aws s3 cp s3://flink-poc-app-nishikesh/certs/client.crt /tmp/client.crt
aws s3 cp s3://flink-poc-app-nishikesh/certs/client.key /tmp/client.key
echo "=== Certs downloaded ==="
ls -la /tmp/*.crt /tmp/*.key
