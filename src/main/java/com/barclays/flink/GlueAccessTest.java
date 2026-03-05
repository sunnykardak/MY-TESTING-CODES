package com.barclays.poc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.PrintWriter;
import java.io.StringWriter;

public class GlueTableLister {

    private static final Logger LOG = LoggerFactory.getLogger(GlueTableLister.class);

    private static final String CATALOG_ID = "767397940910";
    private static final String REGION = "eu-west-1";
    private static final String DATABASE_NAME = "fdk_uk_customer_db_iceberg";
    private static final String S3_OUTPUT_BUCKET = "flink-poc-app-nishikesh";
    private static final String S3_OUTPUT_KEY = "output/glue-table-list-result.txt";

    public static void main(String[] args) throws Exception {
        LOG.info("=== GLUE TABLE LISTER START ===");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new TableListerSource()).name("GlueTableLister").print();
        env.execute("Glue-Table-Lister-POC");
    }

    public static class TableListerSource extends RichSourceFunction<String> {

        private static final Logger LOG = LoggerFactory.getLogger(TableListerSource.class);

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            StringBuilder report = new StringBuilder();
            try {
                GlueClient glue = GlueClient.builder()
                        .region(Region.of(REGION))
                        .httpClient(UrlConnectionHttpClient.builder().build())
                        .build();

                // Step 1: Check database
                String msg = "Checking database: " + DATABASE_NAME + " in catalog: " + CATALOG_ID;
                LOG.info(msg);
                report.append(msg).append("\n");
                ctx.collect(msg);

                try {
                    GetDatabaseResponse dbResp = glue.getDatabase(
                            GetDatabaseRequest.builder()
                                    .catalogId(CATALOG_ID)
                                    .name(DATABASE_NAME)
                                    .build());
                    String found = "DATABASE FOUND: " + dbResp.database().name();
                    LOG.info(found);
                    report.append(found).append("\n");
                    ctx.collect(found);
                } catch (Exception e) {
                    String err = "DB ERROR for " + DATABASE_NAME + ": " + e.getMessage();
                    LOG.error(err);
                    report.append(err).append("\n");
                    ctx.collect(err);

                    // Try alternate name
                    String altName = "fdp_uk_customer_db_iceberg";
                    String retryMsg = "Trying alternate: " + altName;
                    LOG.info(retryMsg);
                    report.append(retryMsg).append("\n");
                    ctx.collect(retryMsg);
                    try {
                        GetDatabaseResponse dbResp2 = glue.getDatabase(
                                GetDatabaseRequest.builder()
                                        .catalogId(CATALOG_ID)
                                        .name(altName)
                                        .build());
                        String found2 = "DATABASE FOUND WITH ALT NAME: " + dbResp2.database().name();
                        LOG.info(found2);
                        report.append(found2).append("\n");
                        ctx.collect(found2);
                    } catch (Exception e2) {
                        String err2 = "ALT DB ALSO FAILED: " + e2.getMessage();
                        LOG.error(err2);
                        report.append(err2).append("\n");
                        ctx.collect(err2);
                    }
                }

                // Step 2: List tables
                String step2 = "=== Listing tables ===";
                LOG.info(step2);
                report.append(step2).append("\n");
                ctx.collect(step2);

                int count = 0;
                String nextToken = null;
                do {
                    GetTablesRequest.Builder req = GetTablesRequest.builder()
                            .catalogId(CATALOG_ID)
                            .databaseName(DATABASE_NAME);
                    if (nextToken != null) req.nextToken(nextToken);

                    GetTablesResponse resp = glue.getTables(req.build());
                    for (Table t : resp.tableList()) {
                        count++;
                        String info = "Table " + count + ": " + t.name() + " | Type: " + t.tableType();
                        LOG.info(info);
                        report.append(info).append("\n");
                        ctx.collect(info);
                    }
                    nextToken = resp.nextToken();
                } while (nextToken != null);

                // Step 3: Summary
                String summary = "===== TOTAL TABLES FOUND: " + count + " =====";
                LOG.info(summary);
                report.append(summary).append("\n");
                ctx.collect(summary);

                glue.close();

                // Write to S3
                writeToS3(report.toString());

            } catch (Exception e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String fatal = "FATAL: " + e.getMessage() + "\n" + sw.toString();
                LOG.error(fatal);
                report.append(fatal);
                ctx.collect(fatal);
                writeToS3(report.toString());
            }
        }

        private void writeToS3(String content) {
            try {
                S3Client s3 = S3Client.builder()
                        .region(Region.of(REGION))
                        .httpClient(UrlConnectionHttpClient.builder().build())
                        .build();
                s3.putObject(
                        PutObjectRequest.builder()
                                .bucket(S3_OUTPUT_BUCKET)
                                .key(S3_OUTPUT_KEY)
                                .contentType("text/plain")
                                .build(),
                        RequestBody.fromString(content));
                LOG.info("Results written to s3://" + S3_OUTPUT_BUCKET + "/" + S3_OUTPUT_KEY);
                s3.close();
            } catch (Exception e) {
                LOG.error("S3 write failed: " + e.getMessage());
            }
        }

        @Override
        public void cancel() {}
    }
}
