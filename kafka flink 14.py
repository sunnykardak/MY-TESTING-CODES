Perfect! Here's the complete step-by-step guide:

---

# Step 1 — Create Maven Project

Create new project structure:
```
bsp-kafka-flink/
├── pom.xml
├── src/main/java/com/barclays/flink/
│   └── KafkaToS3Job.java
└── src/main/resources/
    └── ssl/
        ├── ca-issuing-bundle.crt
        ├── client.crt
        └── client.key
```

---

# Step 2 — pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">

    <modelVersion>4.0.0</modelVersion>
    <groupId>com.barclays.flink</groupId>
    <artifactId>bsp-kafka-flink</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <properties>
        <java.version>21</java.version>
        <flink.version>1.18.1</flink.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
    </properties>

    <dependencies>

        <!-- Flink Streaming -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Flink Clients -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-clients</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Flink Kafka Connector -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka</artifactId>
            <version>3.1.0-1.18</version>
        </dependency>

        <!-- Flink S3 Hadoop -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-s3-fs-hadoop</artifactId>
            <version>${flink.version}</version>
        </dependency>

        <!-- Jackson JSON -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <version>2.15.2</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.barclays.flink.KafkaToS3Job</mainClass>
                                </transformer>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

</project>
```

---

# Step 3 — KafkaToS3Job.java

```java
package com.barclays.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.util.Properties;

public class KafkaToS3Job {

    // ── KAFKA CONFIG ──────────────────────────────────────────────
    private static final String BOOTSTRAP_SERVERS =
        "kafka.bsp.buk.421850845486.aws.intranet:9092";
    private static final String TOPIC =
        "buk-sdwh-digital-poc";
    private static final String GROUP_ID =
        "snsvc0067919";

    // ── S3 OUTPUT ─────────────────────────────────────────────────
    // CHANGE THIS to your bucket
    private static final String S3_PATH =
        "s3://YOUR-BUCKET-NAME/kafka-output/";

    // ── CERT PATHS on AWS Managed Flink ──────────────────────────
    // Certs will be uploaded to S3 and mounted at this path
    private static final String CERT_BASE =
        "/opt/flink/usrlib/certs";

    public static void main(String[] args) throws Exception {

        // ── 1. Environment ────────────────────────────────────────
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpointing REQUIRED for FileSink
        env.enableCheckpointing(60_000);
        env.setParallelism(1);

        // ── 2. SSL Properties ─────────────────────────────────────
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("security.protocol", "SSL");
        kafkaProps.setProperty("ssl.ca.location",
            CERT_BASE + "/ca-issuing-bundle.crt");
        kafkaProps.setProperty("ssl.certificate.location",
            CERT_BASE + "/client.crt");
        kafkaProps.setProperty("ssl.key.location",
            CERT_BASE + "/client.key");
        kafkaProps.setProperty("ssl.key.password", "bukpass");
        kafkaProps.setProperty(
            "ssl.endpoint.identification.algorithm", "none");

        // ── 3. Kafka Source ───────────────────────────────────────
        KafkaSource<String> source = KafkaSource
            .<String>builder()
            .setBootstrapServers(BOOTSTRAP_SERVERS)
            .setTopics(TOPIC)
            .setGroupId(GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(kafkaProps)
            .build();

        // ── 4. Stream ─────────────────────────────────────────────
        DataStream<String> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "BSP Kafka Source"
        );

        // Print to logs for debugging
        stream.print("MSG");

        // ── 5. S3 Sink ────────────────────────────────────────────
        FileSink<String> s3Sink = FileSink
            .forRowFormat(
                new Path(S3_PATH),
                new SimpleStringEncoder<String>("UTF-8")
            )
            .withBucketAssigner(
                new DateTimeBucketAssigner<>("yyyy-MM-dd--HH")
            )
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(15))
                    .withInactivityInterval(Duration.ofMinutes(5))
                    .withMaxPartSize(128L * 1024L * 1024L)
                    .build()
            )
            .build();

        stream.sinkTo(s3Sink);

        // ── 6. Execute ────────────────────────────────────────────
        env.execute("BSP Kafka → S3 Flink Job");
    }
}
```

---

# Step 4 — Upload Certs to S3

```cmd
aws s3 cp ca-issuing-bundle.crt s3://YOUR-BUCKET/certs/ca-issuing-bundle.crt
aws s3 cp client.crt s3://YOUR-BUCKET/certs/client.crt
aws s3 cp client.key s3://YOUR-BUCKET/certs/client.key
```

---

# Step 5 — Build JAR

```cmd
cd bsp-kafka-flink
mvn clean package -DskipTests
```

Then upload JAR:
```cmd
aws s3 cp target/bsp-kafka-flink-1.0.0.jar s3://YOUR-BUCKET/jars/bsp-kafka-flink.jar
```

---

# Step 6 — Create AWS Managed Flink Application

Go to **AWS Console → Kinesis Data Analytics → Create Application**

```
Application name:    bsp-kafka-to-s3
Runtime:             Apache Flink 1.18
Template:            Development (for testing)
                     OR Production (for live)
```

---

# Step 7 — Configure Application

Under **Application Configuration**:

```
Code location (JAR):
  s3://YOUR-BUCKET/jars/bsp-kafka-flink.jar

Main class:
  com.barclays.flink.KafkaToS3Job
```

Under **Runtime Properties** add:
```
Group ID:   FlinkApplicationProperties
Key:        ssl.certs.path
Value:      /opt/flink/usrlib/certs
```

---

# Step 8 — IAM Role Policy

Attach this to your Managed Flink IAM role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:AbortMultipartUpload",
        "s3:ListBucketMultipartUploads",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-NAME",
        "arn:aws:s3:::YOUR-BUCKET-NAME/*"
      ]
    },
    {
      "Sid": "FlinkLogging",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ],
      "Resource": "*"
    }
  ]
}
```

---

# Step 9 — VPC Configuration (CRITICAL)

Since BSP Kafka is on Barclays intranet, Managed Flink **must be in the same VPC**:

```
VPC:              Select your Barclays-connected VPC
Subnets:          Select private subnets
Security Groups:  Allow outbound TCP port 9092
                  to 28.45.10.24/23
```

---

# Step 10 — Expected S3 Output

```
s3://YOUR-BUCKET/kafka-output/
  └── 2026-02-26--12/
      ├── part-0-0
      ├── part-0-1
      └── part-0-2
```

---

# Summary Checklist

```
✅ Step 1 - Create Maven project
✅ Step 2 - Copy pom.xml
✅ Step 3 - Copy KafkaToS3Job.java
✅ Step 4 - Upload certs to S3
✅ Step 5 - Build JAR → upload to S3
✅ Step 6 - Create Managed Flink app
✅ Step 7 - Configure JAR + main class
✅ Step 8 - Set IAM role permissions
✅ Step 9 - Configure VPC settings
✅ Step 10 - Start application
```

**What is your S3 bucket name so I can update the code?**