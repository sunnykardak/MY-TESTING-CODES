package com.barclays.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink Application to test:
 * 1. STS - verify which IAM identity Flink is running as
 * 2. Glue - verify access to cross-account database fdk_uk_customer_db_iceberg
 */
public class GlueAccessTest {

    // ====================================================
    // CONFIGURATION - update these if needed
    // ====================================================
    private static final String AWS_REGION      = "eu-west-1";
    private static final String GLUE_DATABASE   = "fdk_uk_customer_db_iceberg";
    // Optional: set catalog ID if cross-account (the account that owns the Glue catalog)
    // Leave empty string "" to use default (current account)
    private static final String GLUE_CATALOG_ID = "";
    // ====================================================

    public static void main(String[] args) throws Exception {

        // --- Step 1: Collect all test results ---
        List<String> results = new ArrayList<>();

        // --- Step 2: STS Identity Check ---
        results.add("=== STS IDENTITY CHECK ===");
        try {
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(AWS_REGION))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build();

            GetCallerIdentityResponse identity = stsClient.getCallerIdentity();
            results.add("SUCCESS - Account  : " + identity.account());
            results.add("SUCCESS - UserId   : " + identity.userId());
            results.add("SUCCESS - ARN      : " + identity.arn());
            stsClient.close();

        } catch (Exception e) {
            results.add("FAILED - STS Error: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        }

        // --- Step 3: Glue Database Check ---
        results.add("");
        results.add("=== GLUE DATABASE CHECK ===");
        results.add("Target database: " + GLUE_DATABASE);
        results.add("Region        : " + AWS_REGION);

        try {
            GlueClient glueClient = GlueClient.builder()
                    .region(Region.of(AWS_REGION))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build();

            // Build GetDatabase request
            GetDatabaseRequest.Builder dbRequestBuilder = GetDatabaseRequest.builder()
                    .name(GLUE_DATABASE);

            // Set catalog ID only if provided (cross-account scenario)
            if (!GLUE_CATALOG_ID.isEmpty()) {
                dbRequestBuilder.catalogId(GLUE_CATALOG_ID);
                results.add("Using catalog ID: " + GLUE_CATALOG_ID);
            }

            GetDatabaseResponse dbResponse = glueClient.getDatabase(dbRequestBuilder.build());
            Database db = dbResponse.database();

            results.add("SUCCESS - Database found!");
            results.add("  Name       : " + db.name());
            results.add("  Location   : " + (db.locationUri() != null ? db.locationUri() : "N/A"));
            results.add("  Description: " + (db.description() != null ? db.description() : "N/A"));
            results.add("  CatalogId  : " + (db.catalogId() != null ? db.catalogId() : "N/A"));

            // --- Step 4: List Tables ---
            results.add("");
            results.add("=== GLUE TABLE LISTING ===");
            try {
                GetTablesRequest.Builder tablesRequestBuilder = GetTablesRequest.builder()
                        .databaseName(GLUE_DATABASE);

                if (!GLUE_CATALOG_ID.isEmpty()) {
                    tablesRequestBuilder.catalogId(GLUE_CATALOG_ID);
                }

                GetTablesResponse tablesResponse = glueClient.getTables(tablesRequestBuilder.build());
                List<Table> tables = tablesResponse.tableList();

                if (tables.isEmpty()) {
                    results.add("No tables found in database (database is empty)");
                } else {
                    results.add("SUCCESS - Found " + tables.size() + " table(s):");
                    for (Table table : tables) {
                        results.add("  TABLE: " + table.name()
                                + " | Type: " + (table.tableType() != null ? table.tableType() : "N/A")
                                + " | Location: " + (table.storageDescriptor() != null
                                        && table.storageDescriptor().location() != null
                                        ? table.storageDescriptor().location() : "N/A"));
                    }
                }

                // Handle pagination if more tables exist
                String nextToken = tablesResponse.nextToken();
                int pageCount = 1;
                while (nextToken != null && pageCount < 5) { // max 5 pages to avoid infinite loop
                    GetTablesRequest.Builder nextPageBuilder = GetTablesRequest.builder()
                            .databaseName(GLUE_DATABASE)
                            .nextToken(nextToken);
                    if (!GLUE_CATALOG_ID.isEmpty()) {
                        nextPageBuilder.catalogId(GLUE_CATALOG_ID);
                    }
                    GetTablesResponse nextPage = glueClient.getTables(nextPageBuilder.build());
                    for (Table table : nextPage.tableList()) {
                        results.add("  TABLE: " + table.name()
                                + " | Type: " + (table.tableType() != null ? table.tableType() : "N/A"));
                    }
                    nextToken = nextPage.nextToken();
                    pageCount++;
                }

            } catch (Exception tableEx) {
                results.add("FAILED - Table listing error: "
                        + tableEx.getClass().getSimpleName() + " - " + tableEx.getMessage());
            }

            glueClient.close();

        } catch (EntityNotFoundException e) {
            results.add("FAILED - Database not found (EntityNotFoundException)");
            results.add("  This means: database name is wrong, OR cross-account catalog ID needed");
            results.add("  Detail: " + e.getMessage());

        } catch (AccessDeniedException e) {
            results.add("FAILED - Access denied to Glue database");
            results.add("  This means: IAM role lacks glue:GetDatabase permission");
            results.add("  OR: Resource-based policy on Glue catalog blocks this role");
            results.add("  Detail: " + e.getMessage());

        } catch (Exception e) {
            results.add("FAILED - Unexpected error: " + e.getClass().getSimpleName());
            results.add("  Detail: " + e.getMessage());
        }

        results.add("");
        results.add("=== TEST COMPLETE ===");

        // --- Step 5: Run through Flink stream so results appear in CloudWatch ---
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> resultStream = env.fromCollection(results);

        // Print to stdout -> appears in CloudWatch TaskManager logs
        resultStream
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String line) throws Exception {
                        // Using System.out so it's visible in both stdout and Flink logs
                        System.out.println("[GLUE-TEST] " + line);
                        return line;
                    }
                })
                .print();

        env.execute("Glue Access Test - " + GLUE_DATABASE);
    }
}
