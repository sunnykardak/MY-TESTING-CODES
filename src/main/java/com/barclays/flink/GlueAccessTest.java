package com.barclays.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink application to test Glue database access via service role.
 * Writes results DIRECTLY to CloudWatch Logs (not relying on Flink stdout).
 *
 * Target database: fdk_uk_customer_db_iceberg
 * Region: eu-west-1
 * Service role: svc-role-real-time-transformation-poc
 */
public class GlueAccessTest {

    private static final Logger LOG = LoggerFactory.getLogger(GlueAccessTest.class);

    // ===== CONFIGURATION =====
    private static final String DATABASE_NAME = "fdk_uk_customer_db_iceberg";
    private static final String REGION = "eu-west-1";

    // Cross-account catalog ID (12-digit AWS account ID that owns the database)
    // Set this once Anishvaran provides it. Leave null for same-account.
    private static final String CATALOG_ID = null; // e.g. "123456789012"

    // CloudWatch - reuse the existing log group, new stream for our output
    private static final String LOG_GROUP = "/aws/kinesis-analytics/DB-testing-app";
    private static final String LOG_STREAM = "glue-access-test-output";

    public static void main(String[] args) throws Exception {

        LOG.info("=== GLUE ACCESS TEST - STARTING ===");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.fromElements("RUN_ALL_TESTS");
        DataStream<String> results = source.map(new GlueTestMapper());
        results.print();

        env.execute("Glue-Access-Test-Job");
    }

    /**
     * Performs all AWS tests and writes results directly to CloudWatch.
     */
    public static class GlueTestMapper implements MapFunction<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(GlueTestMapper.class);

        @Override
        public String map(String trigger) throws Exception {
            List<String> messages = new ArrayList<>();

            messages.add("====================================================");
            messages.add("  GLUE ACCESS TEST - EXECUTION STARTED");
            messages.add("  Database: " + DATABASE_NAME);
            messages.add("  Region:   " + REGION);
            messages.add("  CatalogId: " + (CATALOG_ID != null ? CATALOG_ID : "default (same account)"));
            messages.add("  Timestamp: " + java.time.Instant.now().toString());
            messages.add("====================================================");

            // --- Test 1: STS Identity ---
            messages.add("");
            messages.add("--- TEST 1: STS GetCallerIdentity ---");
            try {
                testStsIdentity(messages);
            } catch (Exception e) {
                messages.add("  FAILED: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }

            // --- Test 2: Glue GetDatabase ---
            messages.add("");
            messages.add("--- TEST 2: Glue GetDatabase ---");
            try {
                testGlueDatabase(messages);
            } catch (Exception e) {
                messages.add("  FAILED: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }

            // --- Test 3: Glue GetTables ---
            messages.add("");
            messages.add("--- TEST 3: Glue GetTables ---");
            try {
                testGlueTables(messages);
            } catch (Exception e) {
                messages.add("  FAILED: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }

            messages.add("");
            messages.add("====================================================");
            messages.add("  ALL TESTS COMPLETED");
            messages.add("====================================================");

            // --- Write to CloudWatch directly ---
            try {
                writeToCloudWatch(messages);
                messages.add("  >> Written to CloudWatch: " + LOG_GROUP + " / " + LOG_STREAM);
            } catch (Exception e) {
                messages.add("  >> CloudWatch write failed: " + e.getMessage());
            }

            String fullOutput = String.join("\n", messages);
            System.out.println(fullOutput);
            return fullOutput;
        }

        private void testStsIdentity(List<String> messages) {
            try (StsClient stsClient = StsClient.builder()
                    .region(Region.of(REGION))
                    .build()) {

                GetCallerIdentityResponse identity = stsClient.getCallerIdentity();

                messages.add("  SUCCESS - Identity confirmed");
                messages.add("  Account:  " + identity.account());
                messages.add("  ARN:      " + identity.arn());
                messages.add("  UserId:   " + identity.userId());
            }
        }

        private void testGlueDatabase(List<String> messages) {
            try (GlueClient glueClient = GlueClient.builder()
                    .region(Region.of(REGION))
                    .build()) {

                GetDatabaseRequest.Builder req = GetDatabaseRequest.builder()
                        .name(DATABASE_NAME);

                if (CATALOG_ID != null && !CATALOG_ID.isEmpty()) {
                    req.catalogId(CATALOG_ID);
                    messages.add("  Using CatalogId: " + CATALOG_ID);
                }

                GetDatabaseResponse response = glueClient.getDatabase(req.build());

                messages.add("  SUCCESS - Database accessible!");
                messages.add("  DB Name:        " + response.database().name());
                messages.add("  Description:    " + response.database().description());
                messages.add("  LocationUri:    " + response.database().locationUri());
                messages.add("  CreateTime:     " + response.database().createTime());
                messages.add("  CatalogId:      " + response.database().catalogId());

                if (response.database().parameters() != null) {
                    messages.add("  Parameters:");
                    response.database().parameters().forEach((k, v) ->
                        messages.add("    " + k + " = " + v)
                    );
                }
            }
        }

        private void testGlueTables(List<String> messages) {
            try (GlueClient glueClient = GlueClient.builder()
                    .region(Region.of(REGION))
                    .build()) {

                GetTablesRequest.Builder req = GetTablesRequest.builder()
                        .databaseName(DATABASE_NAME)
                        .maxResults(50);

                if (CATALOG_ID != null && !CATALOG_ID.isEmpty()) {
                    req.catalogId(CATALOG_ID);
                }

                GetTablesResponse response = glueClient.getTables(req.build());

                messages.add("  SUCCESS - Tables retrieved!");
                messages.add("  Total tables: " + response.tableList().size());
                messages.add("  ----------------------------------------");

                int idx = 1;
                for (Table table : response.tableList()) {
                    messages.add("  Table " + idx++ + ":");
                    messages.add("    Name:         " + table.name());
                    messages.add("    TableType:    " + table.tableType());
                    messages.add("    CreateTime:   " + table.createTime());
                    messages.add("    UpdateTime:   " + table.updateTime());
                    messages.add("    Owner:        " + table.owner());

                    if (table.storageDescriptor() != null) {
                        messages.add("    Location:     " + table.storageDescriptor().location());
                        messages.add("    InputFormat:  " + table.storageDescriptor().inputFormat());

                        if (table.storageDescriptor().columns() != null) {
                            messages.add("    Columns (" + table.storageDescriptor().columns().size() + "):");
                            table.storageDescriptor().columns().forEach(col ->
                                messages.add("      - " + col.name() + " (" + col.type() + ")")
                            );
                        }
                    }
                    messages.add("  ----------------------------------------");
                }
            }
        }

        /**
         * Write results directly to CloudWatch Logs using AWS SDK.
         */
        private void writeToCloudWatch(List<String> messages) {
            try (CloudWatchLogsClient logsClient = CloudWatchLogsClient.builder()
                    .region(Region.of(REGION))
                    .build()) {

                // Create log stream (ignore if already exists)
                try {
                    logsClient.createLogStream(CreateLogStreamRequest.builder()
                            .logGroupName(LOG_GROUP)
                            .logStreamName(LOG_STREAM)
                            .build());
                } catch (ResourceAlreadyExistsException e) {
                    // Fine - stream already exists
                }

                // Build log events (each needs unique timestamp)
                long ts = System.currentTimeMillis();
                List<InputLogEvent> events = new ArrayList<>();

                for (int i = 0; i < messages.size(); i++) {
                    events.add(InputLogEvent.builder()
                            .timestamp(ts + i)
                            .message(messages.get(i))
                            .build());
                }

                // Write
                logsClient.putLogEvents(PutLogEventsRequest.builder()
                        .logGroupName(LOG_GROUP)
                        .logStreamName(LOG_STREAM)
                        .logEvents(events)
                        .build());
            }
        }
    }
}
