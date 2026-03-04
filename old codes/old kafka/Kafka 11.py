Good ✅ your Flink app starts.
Now we verify real Kafka → Flink connectivity properly.

I will give you:

1️⃣ Correct Java code (with your real broker + topic)
2️⃣ How to verify connection
3️⃣ What to check in CloudWatch
4️⃣ How to test using your producer.py

No confusion. Follow exactly.

⸻

🚨 IMPORTANT

From your screenshot:

Broker:

kafka.bsp.buk.421850845486.aws.intranet:9092

Topic:

buk-sdwh-digital-poc

Security:

SSL

But your current Java code has NO SSL config.

If Kafka requires SSL → your Flink is NOT connected.

⸻

✅ STEP 1 — Replace Your Java Code Completely

Copy-paste this FULL code:

package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Properties;

public class KafkaFlinkJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("security.protocol", "SSL");
        props.setProperty("ssl.endpoint.identification.algorithm", "");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka.bsp.buk.421850845486.aws.intranet:9092")
                .setTopics("buk-sdwh-digital-poc")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(props)
                .build();

        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        stream.print();

        env.execute("Kafka Flink Connectivity Test");
    }
}

⚠️ This version assumes:
	•	Kafka SSL but no client cert required.
	•	If client cert required → we add keystore later.

⸻

✅ STEP 2 — Rebuild JAR

Inside project folder:

mvn clean package

You will get:

target/flink-kafka-job-1.0.jar


⸻

✅ STEP 3 — Upload JAR to S3

Upload to:

s3://YOUR_BUCKET/flink/flink-kafka-job-1.0.jar


⸻

✅ STEP 4 — Update MSF Application

Go to:

Managed Service for Apache Flink → Your Application
	1.	Stop application
	2.	Update application code
	3.	Select new JAR from S3
	4.	Main class:

com.example.KafkaFlinkJob


	5.	Deploy
	6.	Start

⸻

✅ STEP 5 — Check If Flink Is Connected

Go to:

CloudWatch → Log groups
Select your Flink app logs

Look for:

KafkaSource
Subscribed to topic

If you see:

Connection refused
SSLHandshakeException
UnknownHostException

→ It is NOT connected.

⸻

✅ STEP 6 — Test With Producer

Now run:

python producer.py

Send message:

Hello Barclays Test

Now go to CloudWatch logs.

If connected correctly, you should see:

Hello Barclays Test

Printed by Flink.

⸻

🔎 If Nothing Appears

There are only 4 possible issues:

1️⃣ Wrong VPC (most common)
2️⃣ Security group not allowing outbound 9092
3️⃣ SSL handshake failure
4️⃣ DNS resolution issue (your intranet host)

⸻

🚨 VERY IMPORTANT QUESTION

Your Python code had this:

if "kafka.bsp.buk" in str(host):
    host = "28.45.10.24"

That means your local machine is bypassing DNS.

Flink will NOT do that.

So answer clearly:

Does your Kafka only work because of this manual IP override?

If yes → we must fix DNS or use IP in Flink code.

⸻

🎯 Next Step

Go to CloudWatch logs right now and tell me:

Do you see:

A) Messages printed
B) Kafka connection errors
C) Nothing

Tell me exactly what you see.
