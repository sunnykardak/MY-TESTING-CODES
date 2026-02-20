package com.example.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * FlinkConsumerMain
 * ─────────────────────────────────────────────────────────────────────────────
 * Entry point for the AWS Managed Flink application.
 *
 * Pipeline:
 *   Kinesis Input Stream
 *       └─▶ RecordEnricher   (parse JSON, add timestamps, detect fraud/auth)
 *           └─▶ S3 via StreamingFileSink  (Hive-partitioned: year=/month=/day=/)
 *
 * Runtime properties (set in AWS Console → Application → Runtime Properties):
 *   Group: ApplicationConfig
 *     InputStreamName  →  flink-poc-nishikesh-input-stream
 *     Region           →  eu-west-1
 *     S3OutputPath     →  s3://739275465799-flink-poc-output/processed-logs/
 *     StreamPosition   →  LATEST
 * ─────────────────────────────────────────────────────────────────────────────
 */
public class FlinkConsumerMain {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkConsumerMain.class);

    // Defaults — overridden by runtime properties at deploy time
    static final String DEFAULT_INPUT_STREAM = "flink-poc-nishikesh-input-stream";
    static final String DEFAULT_REGION       = "eu-west-1";
    static final String DEFAULT_S3_OUTPUT    = "s3://739275465799-flink-poc-output/processed-logs/";
    static final String DEFAULT_STREAM_POS   = ConsumerConfigConstants.InitialPosition.LATEST.name();

    public static void main(String[] args) throws Exception {

        // ── 1. Load runtime properties ────────────────────────────────────────
        Map<String, Properties> appProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties appConfig = appProperties.getOrDefault("ApplicationConfig", new Properties());

        String inputStream    = appConfig.getProperty("InputStreamName", DEFAULT_INPUT_STREAM);
        String region         = appConfig.getProperty("Region",          DEFAULT_REGION);
        String s3OutputPath   = appConfig.getProperty("S3OutputPath",    DEFAULT_S3_OUTPUT);
        String streamPosition = appConfig.getProperty("StreamPosition",  DEFAULT_STREAM_POS);

        LOG.info("=== Flink Consumer Starting ===");
        LOG.info("  Input stream : {}", inputStream);
        LOG.info("  Region       : {}", region);
        LOG.info("  S3 output    : {}", s3OutputPath);
        LOG.info("  Position     : {}", streamPosition);

        // ── 2. Flink environment ──────────────────────────────────────────────
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000); // checkpoint every 60 seconds

        // ── 3. Kinesis source ─────────────────────────────────────────────────
        Properties kinesisProps = new Properties();
        kinesisProps.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        kinesisProps.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, streamPosition);

        // EFO = Enhanced Fan-Out: dedicated 2 MB/s per shard per consumer
        kinesisProps.setProperty(
            ConsumerConfigConstants.RECORD_PUBLISHER_TYPE,
            ConsumerConfigConstants.RecordPublisherType.EFO.name()
        );
        kinesisProps.setProperty(
            ConsumerConfigConstants.EFO_CONSUMER_NAME,
            "flink-poc-efo-consumer"
        );

        FlinkKinesisConsumer<String> source = new FlinkKinesisConsumer<>(
            inputStream,
            new SimpleStringSchema(),
            kinesisProps
        );

        DataStream<String> rawStream = env
            .addSource(source)
            .name("KinesisSource[" + inputStream + "]");

        // ── 4. Enrich & process ───────────────────────────────────────────────
        DataStream<String> processedStream = rawStream
            .map(new RecordEnricher())
            .name("RecordEnricher")
            .filter(r -> r != null && !r.isEmpty())
            .name("FilterNulls");

        // ── 5. S3 sink ────────────────────────────────────────────────────────
        // Layout: s3://<bucket>/processed-logs/year=YYYY/month=MM/day=DD/part-*.json
        StreamingFileSink<String> s3Sink = StreamingFileSink
            .<String>forRowFormat(
                new Path(s3OutputPath),
                new SimpleStringEncoder<>("UTF-8")
            )
            .withBucketAssigner(new DatePartitionBucketAssigner())
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
                    .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                    .withMaxPartSize(128 * 1024 * 1024L)   // 128 MB
                    .build()
            )
            .build();

        processedStream.addSink(s3Sink).name("S3Sink");

        // ── 6. Execute ────────────────────────────────────────────────────────
        env.execute("Flink POC — Kinesis → Enrich → S3");
    }
}
