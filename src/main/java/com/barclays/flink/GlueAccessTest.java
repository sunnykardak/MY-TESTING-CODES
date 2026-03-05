package com.barclays.poc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Table;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * GlueTableLister - List all tables in the cross-account Glue database
 *
 * This Flink app runs as svc-role-real-time-transformation-poc which has:
 *   - glue:Get*, glue:SearchTables, glue:List* on Resource: "*"
 *   - Trust policy allows kinesisanalytics.amazonaws.com (Managed Flink)
 *
 * It connects to the Glue catalog in account 767397940910 and lists
 * all tables in fdk_uk_customer_db_iceberg (or fdp_uk_customer_db_iceberg).
 *
 * Output goes to:
 *   1. CloudWatch Logs (via LOG.info)
 *   2. S3 file (as a backup if CloudWatch logging has issues)
 */
public class GlueTableLister {

    private static final Logger LOG = LoggerFactory.getLogger(GlueTableLister.class);

    // ========================================================================
    // CONFIGURATION - UPDATE THESE
    // ========================================================================

    // The AWS Account ID that owns the Glue catalog (the other account)
    private static final String CATALOG_ID = "767397940910";

    // Region
    private static final String REGION = "eu-west-1";

    // The database name - TRY BOTH if one fails:
    //   "fdk_uk_customer_db_iceberg"  (original name you were given)
    //   "fdp_uk_customer_db_iceberg"  (what appeared in your Athena query)
    private static final String DATABASE_NAME = "fdk_uk_customer_db_iceberg";

    // S3 path to write results as backup (in case CloudWatch logs aren't visible)
    private static final String S3_OUTPUT_BUCKET = "flink-poc-app-nishikesh";
    private static final String S3_OUTPUT_KEY = "output/glue-table-list-result.txt";

    // ========================================================================

    public static void main(String[] args) throws Exception {

        LOG.info("====================================================");
        LOG.info("  GLUE TABLE LISTER - Cross Account Access POC");
        LOG.info("====================================================");
        LOG.info("Catalog ID (Account): {}", CATALOG_ID);
        LOG.info("Region: {}", REGION);
        LOG.info("Database: {}", DATABASE_NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Add a source that does the Glue lookup
        env.addSource(new GlueTableListerSource(CATALOG_ID, REGION, DATABASE_NAME,
                S3_OUTPUT_BUCKET, S3_OUTPUT_KEY))
            .name("GlueTableLister")
            .print();  // This sends output to taskmanager stdout/CloudWatch

        env.execute("Glue-Table-Lister-POC");
    }

    /**
     * A Flink source that queries the Glue catalog and emits table info as strings.
     * Using a Source so Flink has a valid job graph (required for Managed Flink).
     */
    public static class GlueTableListerSource extends RichSourceFunction<String> {

        private static final Logger LOG = LoggerFactory.getLogger(GlueTableListerSource.class);

        private final String catalogId;
        private final String region;
        private final String databaseName;
        private final String s3Bucket;
        private final String s3Key;

        public GlueTableListerSource(String catalogId, String region, String databaseName,
                                     String s3Bucket, String s3Key) {
            this.catalogId = catalogId;
            this.region = region;
            this.databaseName = databaseName;
            this.s3Bucket = s3Bucket;
            this.s3Key = s3Key;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            StringBuilder report = new StringBuilder();

            try {
                // Build Glue client - uses UrlConnectionHttpClient to avoid
                // classloader issues with Managed Flink (same fix as your earlier POC)
                GlueClient glueClient = GlueClient.builder()
                        .region(Region.of(region))
                        .httpClient(UrlConnectionHttpClient.builder().build())
                        .build();

                // --------------------------------------------------------
                // STEP 1: Verify database exists
                // --------------------------------------------------------
                String msg = "=== Step 1: Checking database '" + databaseName +
                        "' in catalog " + catalogId + " ===";
                LOG.info(msg);
                report.append(msg).append("\n");

                try {
                    GetDatabaseResponse dbResponse = glueClient.getDatabase(
                            GetDatabaseRequest.builder()
                                    .catalogId(catalogId)
                                    .name(databaseName)
                                    .build()
                    );

                    String dbInfo = "Database found: " + dbResponse.database().name() +
                            " | Description: " + dbResponse.database().description() +
                            " | LocationUri: " + dbResponse.database().locationUri();
                    LOG.info(dbInfo);
                    report.append(dbInfo).append("\n");
                    ctx.collect(dbInfo);

                } catch (Exception e) {
                    String errMsg = "ERROR accessing database '" + databaseName + "': " + e.getMessage();
                    LOG.error(errMsg);
                    report.append(errMsg).append("\n");
                    ctx.collect(errMsg);

                    // Try the alternative database name
                    String altDbName = databaseName.startsWith("fdk") ?
                            databaseName.replace("fdk", "fdp") :
                            databaseName.replace("fdp", "fdk");

                    String retryMsg = "Trying alternative database name: " + altDbName;
                    LOG.info(retryMsg);
                    report.append(retryMsg).append("\n");
                    ctx.collect(retryMsg);

                    try {
                        GetDatabaseResponse dbResponse2 = glueClient.getDatabase(
                                GetDatabaseRequest.builder()
                                        .catalogId(catalogId)
                                        .name(altDbName)
                                        .build()
                        );
                        String dbInfo2 = "Database found with alt name: " + dbResponse2.database().name();
                        LOG.info(dbInfo2);
                        report.append(dbInfo2).append("\n");
                        ctx.collect(dbInfo2);
                        // If alt name works, we won't re-list tables here, but log it
                    } catch (Exception e2) {
                        String errMsg2 = "Alt database also failed: " + e2.getMessage();
                        LOG.error(errMsg2);
                        report.append(errMsg2).append("\n");
                        ctx.collect(errMsg2);
                    }
                }

                // --------------------------------------------------------
                // STEP 2: List all tables in the database
                // --------------------------------------------------------
                String step2Msg = "\n=== Step 2: Listing tables in " + databaseName + " ===";
                LOG.info(step2Msg);
                report.append(step2Msg).append("\n");

                int totalTables = 0;
                String nextToken = null;

                do {
                    GetTablesRequest.Builder requestBuilder = GetTablesRequest.builder()
                            .catalogId(catalogId)
                            .databaseName(databaseName);

                    if (nextToken != null) {
                        requestBuilder.nextToken(nextToken);
                    }

                    GetTablesResponse tablesResponse = glueClient.getTables(requestBuilder.build());

                    for (Table table : tablesResponse.tableList()) {
                        totalTables++;
                        String tableInfo = String.format(
                                "  Table %d: %-50s | Type: %-15s | Format: %s",
                                totalTables,
                                table.name(),
                                table.tableType() != null ? table.tableType() : "N/A",
                                table.parameters() != null ?
                                        table.parameters().getOrDefault("table_type",
                                                table.parameters().getOrDefault("classification", "N/A"))
                                        : "N/A"
                        );
                        LOG.info(tableInfo);
                        report.append(tableInfo).append("\n");
                        ctx.collect(tableInfo);
                    }

                    nextToken = tablesResponse.nextToken();

                } while (nextToken != null);

                // --------------------------------------------------------
                // STEP 3: Print summary
                // --------------------------------------------------------
                String summary = "\n====================================================\n" +
                        "  RESULT: Found " + totalTables + " tables in " + databaseName + "\n" +
                        "  Catalog ID: " + catalogId + "\n" +
                        "====================================================";
                LOG.info(summary);
                report.append(summary).append("\n");
                ctx.collect(summary);

                // --------------------------------------------------------
                // STEP 4: Write results to S3 as backup
                // --------------------------------------------------------
                writeResultToS3(report.toString());

                glueClient.close();

            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String fullError = "FATAL ERROR: " + e.getMessage() + "\n" + sw.toString();
                LOG.error(fullError);
                report.append(fullError);
                ctx.collect(fullError);
                writeResultToS3(report.toString());
            }
        }

        /**
         * Write results to S3 so you can check even if CloudWatch logs are broken.
         */
        private void writeResultToS3(String content) {
            try {
                software.amazon.awssdk.services.s3.S3Client s3 =
                        software.amazon.awssdk.services.s3.S3Client.builder()
                                .region(Region.of(region))
                                .httpClient(UrlConnectionHttpClient.builder().build())
                                .build();

                s3.putObject(
                        software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                                .bucket(s3Bucket)
                                .key(s3Key)
                                .contentType("text/plain")
                                .build(),
                        software.amazon.awssdk.core.sync.RequestBody.fromString(content)
                );

                LOG.info("Results written to s3://{}/{}", s3Bucket, s3Key);
                s3.close();

            } catch (Exception e) {
                LOG.error("Failed to write results to S3: {}", e.getMessage());
            }
        }

        @Override
        public void cancel() {
            // Nothing to cancel
        }
    }
}
