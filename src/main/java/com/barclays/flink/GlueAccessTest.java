package com.barclays.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
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
 * Flink app to test Glue database access via service role.
 * Uses UrlConnectionHttpClient explicitly (no service discovery needed).
 * Writes results directly to CloudWatch Logs.
 */
public class GlueAccessTest {

    private static final Logger LOG = LoggerFactory.getLogger(GlueAccessTest.class);

    private static final String DATABASE_NAME = "fdk_uk_customer_db_iceberg";
    private static final String REGION = "eu-west-1";
    private static final String CATALOG_ID = null; // Set to 12-digit account ID if cross-account
    private static final String LOG_GROUP = "/aws/kinesis-analytics/DB-testing-app";
    private static final String LOG_STREAM = "glue-access-test-output";

    public static void main(String[] args) throws Exception {
        LOG.info("=== GLUE ACCESS TEST STARTING ===");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.fromElements("RUN_ALL_TESTS");
        DataStream<String> results = source.map(new GlueTestMapper());
        results.print();

        env.execute("Glue-Access-Test-Job");
    }

    public static class GlueTestMapper implements MapFunction<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(GlueTestMapper.class);

        @Override
        public String map(String trigger) throws Exception {
            List<String> messages = new ArrayList<>();

            messages.add("====================================================");
            messages.add("  GLUE ACCESS TEST - STARTED");
            messages.add("  Database:  " + DATABASE_NAME);
            messages.add("  Region:    " + REGION);
            messages.add("  CatalogId: " + (CATALOG_ID != null ? CATALOG_ID : "default"));
            messages.add("  Time:      " + java.time.Instant.now().toString());
            messages.add("====================================================");

            // Test 1: STS
            messages.add("");
            messages.add("--- TEST 1: STS GetCallerIdentity ---");
            try {
                testSts(messages);
            } catch (Exception e) {
                messages.add("  FAILED: " + e.getClass().getName() + ": " + e.getMessage());
            }

            // Test 2: Glue GetDatabase
            messages.add("");
            messages.add("--- TEST 2: Glue GetDatabase ---");
            try {
                testGlueDatabase(messages);
            } catch (Exception e) {
                messages.add("  FAILED: " + e.getClass().getName() + ": " + e.getMessage());
            }

            // Test 3: Glue GetTables
            messages.add("");
            messages.add("--- TEST 3: Glue GetTables ---");
            try {
                testGlueTables(messages);
            } catch (Exception e) {
                messages.add("  FAILED: " + e.getClass().getName() + ": " + e.getMessage());
            }

            messages.add("");
            messages.add("=== ALL TESTS COMPLETED ===");

            // Write to CloudWatch
            try {
                writeToCloudWatch(messages);
                messages.add(">> Written to: " + LOG_GROUP + " / " + LOG_STREAM);
            } catch (Exception e) {
                messages.add(">> CloudWatch write FAILED: " + e.getClass().getName() + ": " + e.getMessage());
            }

            String output = String.join("\n", messages);
            System.out.println(output);
            return output;
        }

        private void testSts(List<String> msg) {
            // Explicitly use UrlConnectionHttpClient - no service discovery
            StsClient client = StsClient.builder()
                    .region(Region.of(REGION))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build();
            try {
                GetCallerIdentityResponse id = client.getCallerIdentity();
                msg.add("  SUCCESS");
                msg.add("  Account: " + id.account());
                msg.add("  ARN:     " + id.arn());
                msg.add("  UserId:  " + id.userId());
            } finally {
                client.close();
            }
        }

        private void testGlueDatabase(List<String> msg) {
            GlueClient client = GlueClient.builder()
                    .region(Region.of(REGION))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build();
            try {
                GetDatabaseRequest.Builder req = GetDatabaseRequest.builder()
                        .name(DATABASE_NAME);
                if (CATALOG_ID != null && !CATALOG_ID.isEmpty()) {
                    req.catalogId(CATALOG_ID);
                    msg.add("  Using CatalogId: " + CATALOG_ID);
                }

                GetDatabaseResponse resp = client.getDatabase(req.build());

                msg.add("  SUCCESS - Database accessible!");
                msg.add("  Name:        " + resp.database().name());
                msg.add("  Description: " + resp.database().description());
                msg.add("  LocationUri: " + resp.database().locationUri());
                msg.add("  CreateTime:  " + resp.database().createTime());
                msg.add("  CatalogId:   " + resp.database().catalogId());

                if (resp.database().parameters() != null) {
                    msg.add("  Parameters:");
                    resp.database().parameters().forEach((k, v) ->
                        msg.add("    " + k + " = " + v));
                }
            } finally {
                client.close();
            }
        }

        private void testGlueTables(List<String> msg) {
            GlueClient client = GlueClient.builder()
                    .region(Region.of(REGION))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build();
            try {
                GetTablesRequest.Builder req = GetTablesRequest.builder()
                        .databaseName(DATABASE_NAME)
                        .maxResults(50);
                if (CATALOG_ID != null && !CATALOG_ID.isEmpty()) {
                    req.catalogId(CATALOG_ID);
                }

                GetTablesResponse resp = client.getTables(req.build());

                msg.add("  SUCCESS - Tables retrieved!");
                msg.add("  Total: " + resp.tableList().size());
                msg.add("  ----------------------------------------");

                int idx = 1;
                for (Table t : resp.tableList()) {
                    msg.add("  Table " + idx++ + ": " + t.name());
                    msg.add("    Type:       " + t.tableType());
                    msg.add("    CreateTime: " + t.createTime());
                    msg.add("    Owner:      " + t.owner());
                    if (t.storageDescriptor() != null) {
                        msg.add("    Location:   " + t.storageDescriptor().location());
                        if (t.storageDescriptor().columns() != null) {
                            msg.add("    Columns (" + t.storageDescriptor().columns().size() + "):");
                            t.storageDescriptor().columns().forEach(c ->
                                msg.add("      - " + c.name() + " (" + c.type() + ")"));
                        }
                    }
                    msg.add("  ----------------------------------------");
                }
            } finally {
                client.close();
            }
        }

        private void writeToCloudWatch(List<String> messages) {
            CloudWatchLogsClient client = CloudWatchLogsClient.builder()
                    .region(Region.of(REGION))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build();
            try {
                // Create stream (ignore if exists)
                try {
                    client.createLogStream(CreateLogStreamRequest.builder()
                            .logGroupName(LOG_GROUP)
                            .logStreamName(LOG_STREAM)
                            .build());
                } catch (ResourceAlreadyExistsException e) {
                    // OK
                }

                // Build events
                long ts = System.currentTimeMillis();
                List<InputLogEvent> events = new ArrayList<>();
                for (int i = 0; i < messages.size(); i++) {
                    events.add(InputLogEvent.builder()
                            .timestamp(ts + i)
                            .message(messages.get(i))
                            .build());
                }

                client.putLogEvents(PutLogEventsRequest.builder()
                        .logGroupName(LOG_GROUP)
                        .logStreamName(LOG_STREAM)
                        .logEvents(events)
                        .build());
            } finally {
                client.close();
            }
        }
    }
}
