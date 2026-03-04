package com.barclays.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import software.amazon.awssdk.regions.Region;
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

/**
 * Minimal Flink application to test Glue database access via service role.
 * 
 * What this does:
 * 1. Creates a valid Flink job graph (required for Managed Flink to start)
 * 2. In the map operator, calls STS to verify identity
 * 3. Calls Glue to get database details
 * 4. Calls Glue to list tables in the database
 * 5. Prints all results to CloudWatch via Flink logger + System.out
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
    // If cross-account, set the catalog ID (the 12-digit AWS account ID that owns the database)
    // Leave as null to use the default catalog (same account)
    private static final String CATALOG_ID = null; // e.g. "123456789012"

    public static void main(String[] args) throws Exception {

        LOG.info("====================================================");
        LOG.info("  GLUE ACCESS TEST - APPLICATION STARTING");
        LOG.info("====================================================");
        LOG.info("  Database: {}", DATABASE_NAME);
        LOG.info("  Region:   {}", REGION);
        LOG.info("  CatalogId: {}", CATALOG_ID != null ? CATALOG_ID : "default (same account)");
        LOG.info("====================================================");

        System.out.println("====================================================");
        System.out.println("  GLUE ACCESS TEST - APPLICATION STARTING");
        System.out.println("  Database: " + DATABASE_NAME);
        System.out.println("  Region:   " + REGION);
        System.out.println("====================================================");

        // ===== Step 1: Create valid Flink execution environment =====
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ===== Step 2: Create a minimal valid job graph =====
        // Flink REQUIRES at least one source -> operator -> sink
        DataStream<String> source = env.fromElements(
            "TRIGGER_STS_CHECK",
            "TRIGGER_GLUE_DATABASE_CHECK",
            "TRIGGER_GLUE_TABLES_CHECK"
        );

        DataStream<String> results = source.map(new GlueTestMapper());

        // Print results to taskmanager stdout -> CloudWatch
        results.print();

        // ===== Step 3: Execute =====
        LOG.info("Submitting Flink job graph...");
        env.execute("Glue-Access-Test-Job");
    }

    /**
     * Map function that performs the actual AWS API calls.
     * Each input string triggers a different test.
     */
    public static class GlueTestMapper implements MapFunction<String, String> {

        private static final Logger LOG = LoggerFactory.getLogger(GlueTestMapper.class);

        @Override
        public String map(String trigger) throws Exception {
            StringBuilder result = new StringBuilder();
            result.append("\n========================================\n");
            result.append("  TRIGGER: ").append(trigger).append("\n");
            result.append("========================================\n");

            try {
                switch (trigger) {
                    case "TRIGGER_STS_CHECK":
                        result.append(testStsIdentity());
                        break;
                    case "TRIGGER_GLUE_DATABASE_CHECK":
                        result.append(testGlueDatabase());
                        break;
                    case "TRIGGER_GLUE_TABLES_CHECK":
                        result.append(testGlueTables());
                        break;
                    default:
                        result.append("Unknown trigger: ").append(trigger);
                }
            } catch (Exception e) {
                result.append("  EXCEPTION: ").append(e.getClass().getSimpleName()).append("\n");
                result.append("  MESSAGE:   ").append(e.getMessage()).append("\n");

                // Print full stack trace for debugging
                LOG.error("Error during " + trigger, e);
                System.err.println("ERROR during " + trigger + ": " + e.getMessage());
                e.printStackTrace(System.err);
            }

            String output = result.toString();
            LOG.info(output);
            System.out.println(output);
            return output;
        }

        /**
         * Test 1: Verify who we are (STS GetCallerIdentity)
         * This confirms the service role is being used.
         */
        private String testStsIdentity() {
            StringBuilder sb = new StringBuilder();
            sb.append("  [TEST] STS GetCallerIdentity\n");

            try (StsClient stsClient = StsClient.builder()
                    .region(Region.of(REGION))
                    .build()) {

                GetCallerIdentityResponse identity = stsClient.getCallerIdentity();

                sb.append("  SUCCESS - Identity confirmed\n");
                sb.append("  Account:  ").append(identity.account()).append("\n");
                sb.append("  ARN:      ").append(identity.arn()).append("\n");
                sb.append("  UserId:   ").append(identity.userId()).append("\n");
            }

            return sb.toString();
        }

        /**
         * Test 2: Get database details from Glue catalog
         */
        private String testGlueDatabase() {
            StringBuilder sb = new StringBuilder();
            sb.append("  [TEST] Glue GetDatabase\n");
            sb.append("  Database: ").append(DATABASE_NAME).append("\n");

            try (GlueClient glueClient = GlueClient.builder()
                    .region(Region.of(REGION))
                    .build()) {

                GetDatabaseRequest.Builder requestBuilder = GetDatabaseRequest.builder()
                        .name(DATABASE_NAME);

                // If cross-account, specify the catalog ID
                if (CATALOG_ID != null && !CATALOG_ID.isEmpty()) {
                    requestBuilder.catalogId(CATALOG_ID);
                    sb.append("  CatalogId: ").append(CATALOG_ID).append("\n");
                }

                GetDatabaseResponse response = glueClient.getDatabase(requestBuilder.build());

                sb.append("  SUCCESS - Database accessible!\n");
                sb.append("  DB Name:        ").append(response.database().name()).append("\n");
                sb.append("  Description:    ").append(response.database().description()).append("\n");
                sb.append("  LocationUri:    ").append(response.database().locationUri()).append("\n");
                sb.append("  CreateTime:     ").append(response.database().createTime()).append("\n");
                sb.append("  CatalogId:      ").append(response.database().catalogId()).append("\n");

                if (response.database().parameters() != null) {
                    sb.append("  Parameters:\n");
                    response.database().parameters().forEach((k, v) ->
                        sb.append("    ").append(k).append(" = ").append(v).append("\n")
                    );
                }
            }

            return sb.toString();
        }

        /**
         * Test 3: List tables in the database
         */
        private String testGlueTables() {
            StringBuilder sb = new StringBuilder();
            sb.append("  [TEST] Glue GetTables\n");
            sb.append("  Database: ").append(DATABASE_NAME).append("\n");

            try (GlueClient glueClient = GlueClient.builder()
                    .region(Region.of(REGION))
                    .build()) {

                GetTablesRequest.Builder requestBuilder = GetTablesRequest.builder()
                        .databaseName(DATABASE_NAME)
                        .maxResults(50); // Get up to 50 tables

                if (CATALOG_ID != null && !CATALOG_ID.isEmpty()) {
                    requestBuilder.catalogId(CATALOG_ID);
                }

                GetTablesResponse response = glueClient.getTables(requestBuilder.build());

                sb.append("  SUCCESS - Tables retrieved!\n");
                sb.append("  Total tables found: ").append(response.tableList().size()).append("\n");
                sb.append("  ----------------------------------------\n");

                int index = 1;
                for (Table table : response.tableList()) {
                    sb.append("  Table ").append(index++).append(":\n");
                    sb.append("    Name:         ").append(table.name()).append("\n");
                    sb.append("    TableType:    ").append(table.tableType()).append("\n");
                    sb.append("    CreateTime:   ").append(table.createTime()).append("\n");
                    sb.append("    UpdateTime:   ").append(table.updateTime()).append("\n");
                    sb.append("    Owner:        ").append(table.owner()).append("\n");
                    sb.append("    Description:  ").append(table.description()).append("\n");

                    if (table.storageDescriptor() != null) {
                        sb.append("    Location:     ").append(table.storageDescriptor().location()).append("\n");
                        sb.append("    InputFormat:  ").append(table.storageDescriptor().inputFormat()).append("\n");
                        sb.append("    OutputFormat: ").append(table.storageDescriptor().outputFormat()).append("\n");

                        if (table.storageDescriptor().columns() != null) {
                            sb.append("    Columns (").append(table.storageDescriptor().columns().size()).append("):\n");
                            table.storageDescriptor().columns().forEach(col ->
                                sb.append("      - ").append(col.name())
                                  .append(" (").append(col.type()).append(")")
                                  .append(col.comment() != null ? " // " + col.comment() : "")
                                  .append("\n")
                            );
                        }
                    }

                    if (table.parameters() != null && !table.parameters().isEmpty()) {
                        sb.append("    Parameters:\n");
                        table.parameters().forEach((k, v) ->
                            sb.append("      ").append(k).append(" = ").append(v).append("\n")
                        );
                    }

                    sb.append("  ----------------------------------------\n");
                }

                if (response.nextToken() != null) {
                    sb.append("  NOTE: More tables exist (pagination token present)\n");
                }
            }

            return sb.toString();
        }
    }
}
