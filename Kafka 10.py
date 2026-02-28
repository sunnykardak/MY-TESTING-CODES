Perfect.
We are doing Kafka → Flink (Java) → Print (for MSF deployment).

I will give you:
	1.	Folder structure
	2.	Full pom.xml (copy-paste)
	3.	Full Java file (copy-paste)
	4.	Build command
	5.	Deploy steps to MSF

No missing pieces.

⸻

✅ STEP 1 — Create Project Structure

Create folder:

flink-kafka-job

Inside it:

flink-kafka-job
 ├── pom.xml
 └── src
     └── main
         └── java
             └── com
                 └── example
                     └── KafkaFlinkJob.java


⸻

✅ STEP 2 — Copy-Paste FULL pom.xml

Create file:

flink-kafka-job/pom.xml

Paste this:

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
    </properties>

    <dependencies>

        <!-- Flink Streaming -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-streaming-java</artifactId>
            <version>${flink.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Kafka Connector -->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-connector-kafka</artifactId>
            <version>3.0.2-1.18</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>

            <!-- Compiler -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.10.1</version>
                <configuration>
                    <source>${java.version}</source>
                    <target>${java.version}</target>
                </configuration>
            </plugin>

            <!-- Create Fat JAR -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.4.1</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <createDependencyReducedPom>false</createDependencyReducedPom>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.example.KafkaFlinkJob</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>

        </plugins>
    </build>

</project>


⸻

✅ STEP 3 — Copy-Paste FULL Java Code

Create file:

src/main/java/com/example/KafkaFlinkJob.java

Paste this:

package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class KafkaFlinkJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("YOUR_MSK_BROKER:9092")
                .setTopics("your-topic")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        stream
                .map(value -> "Processed: " + value)
                .print();

        env.execute("Kafka Flink MSF Job");
    }
}


⸻

🔥 IMPORTANT

Replace:

YOUR_MSK_BROKER:9092
your-topic

With your actual MSK broker + topic.

Example:

b-1.mskcluster.xxxxx.amazonaws.com:9092
orders-topic


⸻

✅ STEP 4 — Build the JAR

Inside project root folder run:

mvn clean package

After success:

You will get:

target/flink-kafka-job-1.0.jar

This is your fat JAR.

⸻

✅ STEP 5 — Upload to S3

Upload:

flink-kafka-job-1.0.jar

to:

s3://your-bucket/flink/flink-kafka-job-1.0.jar


⸻

✅ STEP 6 — Deploy in MSF
	1.	Go to MSF Application
	2.	Select Flink 1.18
	3.	Application code location → S3
	4.	Provide:

s3://your-bucket/flink/flink-kafka-job-1.0.jar

	5.	Main class:

com.example.KafkaFlinkJob

	6.	Deploy
	7.	Start application

⸻

✅ STEP 7 — Check Logs

Go to:

CloudWatch → Log group
Search:

Processed:

If Kafka has data, you will see output.

⸻

🔥 If It Fails

Most common issues:

• Security group not allowing Kafka access
• IAM role missing MSK permissions
• Wrong broker endpoint

⸻

🚀 Next Upgrade (Tell Me)

Do you want next:
	1.	Kafka → S3 Sink
	2.	Kafka → Cassandra
	3.	Kafka → Snowflake
	4.	Windowing + aggregation example

Tell me exactly.
