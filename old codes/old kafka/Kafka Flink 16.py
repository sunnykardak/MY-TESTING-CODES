package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class KafkaFlinkJob {

```
// ══════════════════════════════════════════════════════════════
// CONFIG - ONLY CHANGE THESE IF NEEDED
// ══════════════════════════════════════════════════════════════

private static final String BOOTSTRAP_SERVERS =
    "kafka.bsp.buk.421850845486.aws.intranet:9092";

private static final String TOPIC =
    "buk-sdwh-digital-poc";

private static final String GROUP_ID =
    "snsvc0067919";

// Certs downloaded from S3 to /tmp by bootstrap script
private static final String CERT_BASE =
    "/tmp";

private static final String KEY_PASSWORD =
    "bukpass";

// ══════════════════════════════════════════════════════════════

public static void main(String[] args) throws Exception {

    // ── Print startup info ────────────────────────────────────
    System.out.println("╔══════════════════════════════════════╗");
    System.out.println("║   BSP Kafka Flink Job Starting...    ║");
    System.out.println("╚══════════════════════════════════════╝");
    System.out.println("Bootstrap : " + BOOTSTRAP_SERVERS);
    System.out.println("Topic     : " + TOPIC);
    System.out.println("Group ID  : " + GROUP_ID);
    System.out.println("Cert Base : " + CERT_BASE);
    System.out.println("CA Cert   : " + CERT_BASE + "/ca-issuing-bundle.crt");
    System.out.println("Client CRT: " + CERT_BASE + "/client.crt");
    System.out.println("Client KEY: " + CERT_BASE + "/client.key");

    // ── 1. Flink Environment ──────────────────────────────────
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(30_000);

    // ── 2. Kafka SSL Properties ───────────────────────────────
    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(
        "security.protocol",                    "SSL");
    kafkaProps.setProperty(
        "ssl.ca.location",
        CERT_BASE + "/ca-issuing-bundle.crt");
    kafkaProps.setProperty(
        "ssl.certificate.location",
        CERT_BASE + "/client.crt");
    kafkaProps.setProperty(
        "ssl.key.location",
        CERT_BASE + "/client.key");
    kafkaProps.setProperty(
        "ssl.key.password",                     KEY_PASSWORD);
    kafkaProps.setProperty(
        "ssl.endpoint.identification.algorithm","");

    System.out.println("✅ Kafka SSL properties configured");

    // ── 3. Build Kafka Source ─────────────────────────────────
    KafkaSource<String> kafkaSource = KafkaSource
        .<String>builder()
        .setBootstrapServers(BOOTSTRAP_SERVERS)
        .setTopics(TOPIC)
        .setGroupId(GROUP_ID)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setProperties(kafkaProps)
        .build();

    System.out.println("✅ Kafka source built");

    // ── 4. Create DataStream ──────────────────────────────────
    DataStream<String> stream = env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(),
        "BSP-Kafka-Source"
    );

    System.out.println("✅ DataStream created");

    // ── 5. Print every message to CloudWatch Logs ─────────────
    stream.map(msg -> {
        System.out.println("📨 MESSAGE RECEIVED: " + msg);
        return "PROCESSED: " + msg;
    }).print("BSP_KAFKA_OUTPUT");

    // ── 6. Execute Job ────────────────────────────────────────
    System.out.println("✅ Submitting Flink job...");
    env.execute("BSP Kafka → CloudWatch Test");
}
```

}
