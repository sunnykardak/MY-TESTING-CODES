#!/bin/bash
# =============================================================
#  BUILD & DEPLOY: Flink Glue Access Test
# =============================================================
#  Prerequisites:
#    - Java 11 (NOT 21!)
#    - Maven 3.x
#    - AWS CLI configured
# =============================================================

set -e

echo "============================================"
echo "  Step 0: Verify Java Version"
echo "============================================"
java -version 2>&1
JAVA_VER=$(java -version 2>&1 | head -1 | awk -F '"' '{print $2}' | cut -d. -f1)
if [ "$JAVA_VER" != "11" ]; then
    echo ""
    echo "  WARNING: You are NOT on Java 11!"
    echo "  Current: Java $JAVA_VER"
    echo "  Managed Flink 1.18 requires Java 11"
    echo ""
    echo "  Fix with:"
    echo "    export JAVA_HOME=/path/to/java-11"
    echo "    export PATH=\$JAVA_HOME/bin:\$PATH"
    echo ""
    echo "  On Mac with sdkman:"
    echo "    sdk use java 11.0.21-amzn"
    echo ""
    read -p "  Continue anyway? (y/N): " confirm
    if [ "$confirm" != "y" ]; then exit 1; fi
fi

echo ""
echo "============================================"
echo "  Step 1: Clean Build"
echo "============================================"
mvn clean package -DskipTests

echo ""
echo "============================================"
echo "  Step 2: Verify JAR"
echo "============================================"
JAR_FILE="target/flink-glue-test-1.0.0.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "  ERROR: JAR not found at $JAR_FILE"
    exit 1
fi

JAR_SIZE=$(ls -lh "$JAR_FILE" | awk '{print $5}')
echo "  JAR file: $JAR_FILE"
echo "  JAR size: $JAR_SIZE"

echo ""
echo "  Checking Main-Class in MANIFEST..."
MAIN_CLASS=$(unzip -p "$JAR_FILE" META-INF/MANIFEST.MF | grep "Main-Class" || echo "NOT FOUND")
echo "  $MAIN_CLASS"

echo ""
echo "  Checking critical classes exist in JAR..."
unzip -l "$JAR_FILE" | grep "GlueAccessTest.class" || echo "  ERROR: GlueAccessTest.class NOT in JAR!"
unzip -l "$JAR_FILE" | grep "shaded/software/amazon" | head -3 || echo "  WARNING: AWS SDK not shaded into JAR"

echo ""
echo "============================================"
echo "  Step 3: Upload to S3"
echo "============================================"

S3_BUCKET="flink-poc-app-nishikesh"
S3_KEY="flink-glue-test/flink-glue-test-1.0.0.jar"
S3_PATH="s3://${S3_BUCKET}/${S3_KEY}"

echo "  Uploading to: $S3_PATH"
aws s3 cp "$JAR_FILE" "$S3_PATH" --region eu-west-1

echo ""
echo "  Verifying upload..."
aws s3 ls "$S3_PATH" --region eu-west-1

echo ""
echo "============================================"
echo "  BUILD & UPLOAD COMPLETE"
echo "============================================"
echo ""
echo "  Next steps in AWS Console:"
echo ""
echo "  1. Go to Amazon Managed Apache Flink"
echo "  2. Create or update application:"
echo "     - Runtime: Apache Flink 1.18"
echo "     - Service role: svc-role-real-time-transformation-poc"
echo "     - S3 bucket: $S3_BUCKET"
echo "     - S3 key:    $S3_KEY"
echo "  3. Under 'Application properties' -> NO properties needed"
echo "  4. Click 'Run' (or 'Run with latest snapshot: disabled')"
echo "  5. Check CloudWatch Logs for output"
echo ""
echo "  Expected output in CloudWatch:"
echo "    - STS identity (account, ARN)"
echo "    - Glue database details OR AccessDeniedException"
echo "    - Table listing OR AccessDeniedException"
echo ""
echo "============================================"
