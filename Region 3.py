package com.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.regions.Region;

public class App {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("===== STARTING FLINK ACCESS TEST =====");

        // Dummy stream so Flink has a job graph
        env.fromElements("test")
                .map(value -> {
                    try {
                        StsClient sts = StsClient.create();
                        System.out.println("Caller Identity:");
                        System.out.println(sts.getCallerIdentity());
                    } catch (Exception e) {
                        System.out.println("STS FAILED:");
                        e.printStackTrace();
                    }

                    try {
                        GlueClient glue = GlueClient.builder()
                                .region(Region.EU_WEST_1)
                                .build();

                        glue.getDatabase(
                                GetDatabaseRequest.builder()
                                        .name("fdk_uk_customer_db_iceberg")
                                        .build()
                        );

                        System.out.println("SUCCESS: Database accessible");

                    } catch (Exception e) {
                        System.out.println("FAILED to access database:");
                        e.printStackTrace();
                    }

                    return value;
                })
                .print();

        env.execute("Flink IAM Access Test");
    }
}
