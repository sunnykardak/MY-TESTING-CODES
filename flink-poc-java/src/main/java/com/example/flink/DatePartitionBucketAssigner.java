package com.example.flink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * DatePartitionBucketAssigner
 * ─────────────────────────────────────────────────────────────────────────────
 * Assigns each output record to a Hive-compatible S3 partition based on the
 * current UTC wall-clock date:
 *
 *   processed-logs/year=2025/month=11/day=18/part-0-0.json
 *
 * This layout is directly compatible with:
 *   - AWS Glue Crawler  (auto-discovers partitions)
 *   - Amazon Athena     (partition pruning without manual MSCK REPAIR TABLE)
 *   - Amazon QuickSight (direct query)
 * ─────────────────────────────────────────────────────────────────────────────
 */
public class DatePartitionBucketAssigner implements BucketAssigner<String, String> {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter FMT =
        DateTimeFormatter
            .ofPattern("'year='yyyy/'month='MM/'day='dd")
            .withZone(ZoneOffset.UTC);

    @Override
    public String getBucketId(String element, Context context) {
        return ZonedDateTime.now(ZoneOffset.UTC).format(FMT);
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
