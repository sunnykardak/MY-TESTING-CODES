C:\devhome\projects\flink-kafka-job\
├── pom.xml                          ← downloaded file
└── src\main\java\com\example\
    └── KafkaFlinkJob.java           ← downloaded file



cd C:\devhome\projects\flink-kafka-job
mvn clean package -DskipTests



aws s3 cp target\flink-kafka-job-1.0.jar s3://flink-poc-app-nishikesh/jars/flink-kafka-job.jar
```

---

## Step 4 — AWS Managed Flink Settings
```
JAR location:  s3://flink-poc-app-nishikesh/jars/flink-kafka-job.jar
Main class:    com.example.KafkaFlinkJob
VPC:           vpc-083e1e0f99846dbb6 (Development VPC)
Subnets:       All 3 Cidr Booster Private Subnets
Security Group: flink-bsp-kafka-sg





package com.example;

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

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Properties;

public class KafkaFlinkJob {

    // ── CHANGE ONLY THESE IF NEEDED ───────────────────────────────
    private static final String BOOTSTRAP_SERVERS =
        "kafka.bsp.buk.421850845486.aws.intranet:9092";

    private static final String TOPIC =
        "buk-sdwh-digital-poc";

    private static final String GROUP_ID =
        "snsvc0067919";

    private static final String S3_BUCKET =
        "flink-poc-app-nishikesh";

    private static final String S3_OUTPUT =
        "s3://flink-poc-app-nishikesh/kafka-output/";

    private static final String CERT_BASE =
        "/tmp";

    private static final String KEY_PASSWORD =
        "bukpass";

    private static final Region AWS_REGION =
        Region.EU_WEST_1;
    // ─────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {

        // ── Step 1: Download certs from S3 to /tmp ────────────────
        downloadCertsFromS3();

        // ── Step 2: Setup Flink environment ───────────────────────
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60_000);

        // ── Step 3: Kafka SSL config ───────────────────────────────
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("security.protocol",   "SSL");
        kafkaProps.setProperty("ssl.ca.location",
            CERT_BASE + "/ca-issuing-bundle.crt");
        kafkaProps.setProperty("ssl.certificate.location",
            CERT_BASE + "/client.crt");
        kafkaProps.setProperty("ssl.key.location",
            CERT_BASE + "/client.key");
        kafkaProps.setProperty("ssl.key.password",    KEY_PASSWORD);
        kafkaProps.setProperty(
            "ssl.endpoint.identification.algorithm",  "");

        // ── Step 4: Kafka Source ───────────────────────────────────
        KafkaSource<String> kafkaSource = KafkaSource
            .<String>builder()
            .setBootstrapServers(BOOTSTRAP_SERVERS)
            .setTopics(TOPIC)
            .setGroupId(GROUP_ID)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(kafkaProps)
            .build();

        // ── Step 5: Create data stream ────────────────────────────
        DataStream<String> stream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "BSP Kafka Source"
        );

        // Print to CloudWatch logs for debugging
        stream.print("KAFKA_MSG");

        // ── Step 6: S3 File Sink ──────────────────────────────────
        FileSink<String> s3Sink = FileSink
            .forRowFormat(
                new Path(S3_OUTPUT),
                new SimpleStringEncoder<String>("UTF-8")
            )
            .withBucketAssigner(
                // Creates folders like: 2026-03-02--12/
                new DateTimeBucketAssigner<>("yyyy-MM-dd--HH")
            )
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    // Roll file every 15 minutes
                    .withRolloverInterval(Duration.ofMinutes(15))
                    // Roll if no data for 5 minutes
                    .withInactivityInterval(Duration.ofMinutes(5))
                    // Roll if file reaches 128MB
                    .withMaxPartSize(128L * 1024L * 1024L)
                    .build()
            )
            .build();

        // ── Step 7: Connect stream to S3 sink ─────────────────────
        stream.sinkTo(s3Sink);

        // ── Step 8: Run the job ───────────────────────────────────
        env.execute("BSP Kafka to S3 - Flink 1.18");
    }

    /**
     * Downloads SSL certificates from S3 to local /tmp directory.
     * Uses IAM Role automatically - no credentials needed.
     */
    private static void downloadCertsFromS3() {
        System.out.println("=== Downloading certs from S3 ===");

        try {
            S3Client s3 = S3Client.builder()
                .region(AWS_REGION)
                .build();

            String[] files = {
                "ca-issuing-bundle.crt",
                "client.crt",
                "client.key"
            };

            for (String fileName : files) {
                String s3Key = "certs/" + fileName;
                String localPath = CERT_BASE + "/" + fileName;

                GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(S3_BUCKET)
                    .key(s3Key)
                    .build();

                s3.getObject(request, Paths.get(localPath));
                System.out.println("Downloaded: " + fileName
                    + " → " + localPath);
            }

            System.out.println("=== All certs downloaded! ===");

        } catch (Exception e) {
            System.err.println("ERROR downloading certs: "
                + e.getMessage());
            throw new RuntimeException(
                "Failed to download certs from S3", e);
        }
    }
}






<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">

    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>flink-kafka-job</artifactId>
    <version>1.0</version>
    <packaging>jar</packaging>

    <properties>
        <flink.version>1.18.1</flink.version>
        <java.version>11</java.version>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
    </properties>

    <repositories>
        <repository>
            <id>central</id>
            <name>Maven Central</name>
            <url>https://repo1.maven.org/maven2</url>
        </repository>
        <repository>
            <id>apache</id>
            <name>Apache Repository</name>
            <url>https://repository.apache.org/content/repositories/releases</url>
        </repository>
    </repositories>

    <dependencies>

        <!-- Flink Streaming - provided by AWS Managed Flink -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Flink Clients - provided by AWS Managed Flink -->
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

        <!-- AWS SDK S3 for downloading certs -->
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>s3</artifactId>
            <version>2.21.0</version>
        </dependency>

        <!-- AWS SDK Core -->
        <dependency>
            <groupId>software.amazon.awssdk</groupId>
            <artifactId>auth</artifactId>
            <version>2.21.0</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>

            <!-- Compiler Plugin -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.10.1</version>
                <configuration>
                    <source>11</source>
                    <target>11</target>
                </configuration>
            </plugin>

            <!-- Shade Plugin - creates fat JAR with all dependencies -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.0</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <createDependencyReducedPom>false</createDependencyReducedPom>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                        <exclude>META-INF/LICENSE*</exclude>
                                        <exclude>META-INF/NOTICE*</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.example.KafkaFlinkJob</mainClass>
                                </transformer>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
                                    <resource>reference.conf</resource>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

        </plugins>
    </build>

</project>