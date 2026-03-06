package com.barclays.poc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * HiveCatalog + Glue cross-account access
 * Based on approach shared by Anishvaran
 */
public class GlueTableLister {

    private static final Logger LOG = LoggerFactory.getLogger(GlueTableLister.class);

    private static final String TARGET_ACCOUNT_ID = "767397940910";
    private static final String TARGET_DATABASE = "fdk_uk_customer_db_iceberg";
    private static final String REGION = "eu-west-1";
    private static final String S3_BUCKET = "flink-poc-app-nishikesh";
    private static final String S3_KEY = "output/glue-hive-catalog-result.txt";

    public static void main(String[] args) throws Exception {

        StringBuilder report = new StringBuilder();
        report.append("=== HiveCatalog + Glue Cross-Account POC ===\n");
        report.append("Target Account: ").append(TARGET_ACCOUNT_ID).append("\n");
        report.append("Target Database: ").append(TARGET_DATABASE).append("\n");
        report.append("Region: ").append(REGION).append("\n\n");

        try {
            LOG.info("=== Starting HiveCatalog + Glue POC ===");

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            // Configure HiveCatalog with Glue as metastore
            Map<String, String> catalogProperties = new HashMap<>();
            catalogProperties.put("type", "hive");
            catalogProperties.put("default-database", TARGET_DATABASE);
            catalogProperties.put("hive.metastore.glue.catalogid", TARGET_ACCOUNT_ID);
            catalogProperties.put("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory");

            report.append("Creating HiveCatalog with Glue config...\n");
            LOG.info("Creating HiveCatalog with Glue config...");

            HiveCatalog catalog = new HiveCatalog(
                    "cross_account_catalog",
                    TARGET_DATABASE,
                    null,
                    null,
                    catalogProperties
            );

            tableEnv.registerCatalog("cross_account_catalog", catalog);
            tableEnv.useCatalog("cross_account_catalog");

            report.append("Catalog registered successfully.\n\n");
            LOG.info("Catalog registered successfully.");

            // List databases
            report.append("=== SHOW DATABASES ===\n");
            try {
                TableResult dbResult = tableEnv.executeSql("SHOW DATABASES");
                var dbIt = dbResult.collect();
                while (dbIt.hasNext()) {
                    String dbName = dbIt.next().getField(0).toString();
                    LOG.info("Database: {}", dbName);
                    report.append("  - ").append(dbName).append("\n");
                }
            } catch (Exception e) {
                String err = "SHOW DATABASES error: " + e.getMessage();
                LOG.error(err);
                report.append(err).append("\n");
            }

            // List tables
            report.append("\n=== SHOW TABLES in ").append(TARGET_DATABASE).append(" ===\n");
            try {
                tableEnv.executeSql("USE `" + TARGET_DATABASE + "`");
                TableResult tableResult = tableEnv.executeSql("SHOW TABLES");
                int count = 0;
                var it = tableResult.collect();
                while (it.hasNext()) {
                    count++;
                    String tableName = it.next().getField(0).toString();
                    LOG.info("Table {}: {}", count, tableName);
                    report.append("  ").append(count).append(". ").append(tableName).append("\n");
                }
                report.append("\n===== TOTAL TABLES: ").append(count).append(" =====\n");
                LOG.info("TOTAL TABLES: {}", count);
            } catch (Exception e) {
                String err = "SHOW TABLES error: " + e.getMessage();
                LOG.error(err);
                report.append(err).append("\n");

                // Try alternate
                String altDb = "fdp_uk_customer_db_iceberg";
                report.append("\nTrying alternate: ").append(altDb).append("\n");
                try {
                    tableEnv.executeSql("USE `" + altDb + "`");
                    TableResult r2 = tableEnv.executeSql("SHOW TABLES");
                    int c2 = 0;
                    var it2 = r2.collect();
                    while (it2.hasNext()) {
                        c2++;
                        report.append("  ").append(c2).append(". ").append(it2.next().getField(0)).append("\n");
                    }
                    report.append("\nTOTAL TABLES (alt): ").append(c2).append("\n");
                } catch (Exception e2) {
                    report.append("Alt also failed: ").append(e2.getMessage()).append("\n");
                }
            }

        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            report.append("FATAL: ").append(e.getMessage()).append("\n").append(sw);
            LOG.error("FATAL: {}", e.getMessage(), e);
        }

        writeToS3(report.toString());
        LOG.info("Done. Check s3://{}/{}", S3_BUCKET, S3_KEY);
    }

    private static void writeToS3(String content) {
        try {
            S3Client s3 = S3Client.builder()
                    .region(Region.of(REGION))
                    .httpClient(UrlConnectionHttpClient.builder().build())
                    .build();
            s3.putObject(PutObjectRequest.builder().bucket(S3_BUCKET).key(S3_KEY)
                    .contentType("text/plain").build(), RequestBody.fromString(content));
            s3.close();
        } catch (Exception e) {
            System.out.println("S3 WRITE FAILED. Results:\n" + content);
        }
    }
}
