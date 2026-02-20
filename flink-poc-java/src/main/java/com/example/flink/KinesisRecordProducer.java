package com.example.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * KinesisRecordProducer
 * ─────────────────────────────────────────────────────────────────────────────
 * Standalone Java application that generates realistic banking / auth log
 * records matching the client's log format (from screenshot) and pushes them
 * to AWS Kinesis Data Stream.
 *
 * This is the Java replacement for the Python producer script.
 * It runs SEPARATELY from the Flink consumer — on your laptop or on EC2.
 *
 * Usage:
 *   java -cp target/flink-poc-complete-1.0.0.jar \
 *        com.example.flink.KinesisRecordProducer \
 *        [--stream <name>] [--region <region>] [--rate <rps>]
 *        [--count <n>] [--batch] [--batch-size <n>] [--dry-run]
 *
 * AWS credentials are resolved automatically via the default provider chain:
 *   1. Environment variables  (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 *   2. ~/.aws/credentials     (aws configure)
 *   3. EC2/ECS instance IAM role  (no credentials needed on EC2)
 *
 * Generated record shape (mirrors screenshot exactly):
 * {
 *   "message":             "<raw ROLB/IBF log line>",
 *   "@timestamp":          "2025-11-18T02:06:19.244Z",
 *   "messageType":         "DATA_MESSAGE",
 *   "subscriptionFilters": ["Send-logs-to-Cloud-Watch-Logs-destination"],
 *   "id":                  "<39-digit numeric ID>",
 *   "logstash":            { "hostname": "ldwdsr00t3tuhi9" },
 *   "jvmId":               "195734",
 *   "Timestamp":           "18-11-2025 02:06:19.243",
 *   "ifetype":             "app",
 *   "time_interval":       "18-11-2025 02:06:19.243",
 *   "event_date":          "2025-11-18",
 *   "ErrorMsgId":          1101,
 *   "error_detail":        "Invalid credentials supplied",
 *   "event_type":          "ROLB-AWS-ACTIVITY",
 *   "auth_status":         "FAILED",
 *   "user_id":             "999999999999",
 *   "ip_address":          "26.55.165.88",
 *   "country":             "IN",
 *   "app_url":             "/olb/authlogin/loginAppContainer.do",
 *   "rolb_id":             "ROLB-75381217301837-7115361",
 *   "ibf_token":           "40bb584a-46e9-475e-80be-9b0813ac5ed9",
 *   "session_ids":         ["-1970162495149732183", "1763431579243"],
 *   "is_failed_auth":      true,
 *   "failed_attempts":     3,
 *   "producer_meta":       { "sent_at": "...", "record_no": 42, "source": "kinesis-java-producer" }
 * }
 * ─────────────────────────────────────────────────────────────────────────────
 */
public class KinesisRecordProducer {

    // ── Defaults ──────────────────────────────────────────────────────────────
    private static final String DEFAULT_STREAM     = "flink-poc-nishikesh-input-stream";
    private static final String DEFAULT_REGION     = "eu-west-1";
    private static final int    DEFAULT_RPS        = 5;
    private static final int    DEFAULT_COUNT      = 0;    // 0 = infinite
    private static final int    DEFAULT_BATCH_SIZE = 25;

    // ── Reference data ────────────────────────────────────────────────────────
    private static final String[] EVENT_TYPES = {
        "ROLB-AWS-ACTIVITY", "IBF-AUTHLO",  "ROLB-LOGIN",
        "ROLB-LOGOUT",       "IBF-SESSION", "ROLB-TXN"
    };
    private static final String[] AUTH_STATUSES = {
        "SUCCESS", "FAILED", "TIMEOUT", "LOCKED", "UNKNOWN"
    };
    // Weights for status selection (matches real-world auth distribution)
    private static final int[] STATUS_WEIGHTS = { 55, 25, 8, 7, 5 };

    private static final String[] HOSTNAMES = {
        "ldwdsr00t3tuhi9", "ldwdsr00t3tuhi10", "appsvr-aws-01",
        "appsvr-aws-02",   "bsp-node-07",      "bsp-node-08",
        "rolb-svc-03",     "rolb-svc-04"
    };
    private static final String[] APP_URLS = {
        "/olb/authlogin/loginAppContainer.do",
        "/olb/dashboard/home",
        "/olb/transfer/fundTransfer.do",
        "/olb/account/statement.do",
        "/olb/session/timeout.do",
        "/ibf/auth/validate",
        "/ibf/session/init"
    };
    private static final String[] COUNTRIES = {
        "IN", "US", "GB", "SG", "AE", "AU", "CA", "DE"
    };
    private static final String[] SUBSCRIPTION_FILTERS = {
        "Send-logs-to-Cloud-Watch-Logs-destination",
        "Send-logs-to-Cloud-Watch-Logs-destination-DR"
    };

    // ErrorMsgId → description  (from screenshot: ErrorMsgId=1101 fsdfdsfds)
    private static final Map<Integer, String> ERROR_CODES = new LinkedHashMap<>();
    static {
        ERROR_CODES.put(1101, "Invalid credentials supplied");
        ERROR_CODES.put(1102, "Account locked after repeated failures");
        ERROR_CODES.put(1103, "Session expired or invalid token");
        ERROR_CODES.put(1104, "Unauthorized access attempt");
        ERROR_CODES.put(1200, "Transaction limit exceeded");
        ERROR_CODES.put(1201, "Insufficient funds");
        ERROR_CODES.put(1202, "Beneficiary validation failed");
        ERROR_CODES.put(1203, "Duplicate transaction detected");
        ERROR_CODES.put(9999, "Unknown system error");
    }
    private static final int[] ERROR_CODE_KEYS = {
        1101, 1102, 1103, 1104, 1200, 1201, 1202, 1203, 9999
    };

    // ── Formatters ────────────────────────────────────────────────────────────
    private static final DateTimeFormatter ISO_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DISPLAY_FMT =
        DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ─────────────────────────────────────────────────────────────────────────
    // Entry point
    // ─────────────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {

        // ── Parse CLI args ────────────────────────────────────────────────────
        String  streamName = DEFAULT_STREAM;
        String  region     = DEFAULT_REGION;
        int     rps        = DEFAULT_RPS;
        int     count      = DEFAULT_COUNT;
        boolean batch      = false;
        int     batchSize  = DEFAULT_BATCH_SIZE;
        boolean dryRun     = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--stream":     streamName = args[++i];                break;
                case "--region":     region     = args[++i];                break;
                case "--rate":       rps        = Integer.parseInt(args[++i]); break;
                case "--count":      count      = Integer.parseInt(args[++i]); break;
                case "--batch-size": batchSize  = Integer.parseInt(args[++i]); break;
                case "--batch":      batch      = true;                     break;
                case "--dry-run":    dryRun     = true;                     break;
                default: break;
            }
        }
        if (batchSize > 500) batchSize = 500;
        if (rps < 1)         rps = 1;

        // ── Banner ────────────────────────────────────────────────────────────
        System.out.println("=".repeat(65));
        System.out.println("  Kinesis Record Producer — Banking Log Generator (Java)");
        System.out.println("=".repeat(65));
        System.out.printf ("  Stream  : %s%n", streamName);
        System.out.printf ("  Region  : %s%n", region);
        System.out.printf ("  Rate    : %d rec/sec%n", rps);
        System.out.printf ("  Count   : %s%n", count == 0 ? "∞  (Ctrl+C to stop)" : count);
        System.out.printf ("  Mode    : %s%n",
            dryRun ? "DRY RUN (stdout only)" : batch ? "BATCH (put_records)" : "SINGLE (put_record)");
        System.out.println("=".repeat(65));

        // ── Kinesis client ────────────────────────────────────────────────────
        KinesisClient kinesis = null;
        if (!dryRun) {
            kinesis = KinesisClient.builder()
                .region(Region.of(region))
                .build();
            // Connectivity check
            try {
                kinesis.describeStreamSummary(
                    DescribeStreamSummaryRequest.builder()
                        .streamName(streamName)
                        .build()
                );
                System.out.printf("%n  [READY] Connected to stream: %s%n%n", streamName);
            } catch (KinesisException e) {
                System.out.printf("  [FATAL] Cannot reach stream '%s': %s%n", streamName, e.getMessage());
                System.out.println("  Check: AWS credentials, stream name, region, IAM permissions.");
                if (kinesis != null) kinesis.close();
                return;
            }
        } else {
            System.out.println("  [DRY RUN] No records will be sent to Kinesis.\n");
        }

        // ── Produce loop ──────────────────────────────────────────────────────
        int  recordNo    = 0;
        int  sentOk      = 0;
        int  sentErr     = 0;
        long intervalMs  = 1000L / rps;

        try {
            if (batch && !dryRun) {
                // ── BATCH mode ────────────────────────────────────────────────
                List<String> batchBuf = new ArrayList<>(batchSize);
                int batchStart = 0;

                while (count == 0 || recordNo < count) {
                    batchBuf.add(buildRecord(recordNo++));
                    if (batchBuf.size() >= batchSize) {
                        long t0   = System.currentTimeMillis();
                        int  sent = sendBatch(kinesis, streamName, batchBuf, batchStart);
                        sentOk   += sent;
                        sentErr  += batchBuf.size() - sent;

                        batchStart = recordNo;
                        batchBuf.clear();

                        // Throttle to target rate
                        long elapsed = System.currentTimeMillis() - t0;
                        long target  = (long) batchSize * 1000L / rps;
                        if (target > elapsed) Thread.sleep(target - elapsed);
                    }
                }
                // Flush remainder
                if (!batchBuf.isEmpty()) {
                    int sent = sendBatch(kinesis, streamName, batchBuf, batchStart);
                    sentOk  += sent;
                    sentErr += batchBuf.size() - sent;
                }

            } else {
                // ── SINGLE / DRY-RUN mode ─────────────────────────────────────
                while (count == 0 || recordNo < count) {
                    long   t0     = System.currentTimeMillis();
                    String record = buildRecord(recordNo);

                    if (dryRun) {
                        // Pretty-print to stdout
                        Object parsed = MAPPER.readValue(record, Object.class);
                        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(parsed));
                        System.out.println("-".repeat(60));
                        sentOk++;
                    } else {
                        boolean ok = sendSingle(kinesis, streamName, record, recordNo);
                        if (ok) sentOk++; else sentErr++;
                    }
                    recordNo++;

                    long elapsed = System.currentTimeMillis() - t0;
                    if (intervalMs > elapsed) Thread.sleep(intervalMs - elapsed);
                }
            }
        } catch (InterruptedException e) {
            System.out.println("\n  [STOPPED] Interrupted.");
        } finally {
            if (kinesis != null) kinesis.close();
        }

        // ── Summary ───────────────────────────────────────────────────────────
        System.out.println("\n" + "=".repeat(65));
        System.out.printf ("  Records attempted : %d%n", recordNo);
        System.out.printf ("  Records sent OK   : %d%n", sentOk);
        System.out.printf ("  Records failed    : %d%n", sentErr);
        System.out.println("=".repeat(65));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Send helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static boolean sendSingle(KinesisClient kinesis, String streamName,
                                      String recordJson, int recordNo) {
        try {
            PutRecordResponse resp = kinesis.putRecord(
                PutRecordRequest.builder()
                    .streamName(streamName)
                    .data(SdkBytes.fromUtf8String(recordJson))
                    .partitionKey(String.valueOf(recordNo % 100))
                    .build()
            );
            String seq = resp.sequenceNumber();
            System.out.printf("  [OK  #%06d] shard=%-25s seq=%.20s…%n",
                recordNo, resp.shardId(), seq);
            return true;
        } catch (KinesisException e) {
            System.out.printf("  [ERR #%06d] %s: %s%n",
                recordNo, e.awsErrorDetails().errorCode(), e.getMessage());
            return false;
        }
    }

    private static int sendBatch(KinesisClient kinesis, String streamName,
                                 List<String> records, int startNo) {
        List<PutRecordsRequestEntry> entries = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            entries.add(
                PutRecordsRequestEntry.builder()
                    .data(SdkBytes.fromUtf8String(records.get(i)))
                    .partitionKey(String.valueOf((startNo + i) % 100))
                    .build()
            );
        }
        try {
            PutRecordsResponse resp = kinesis.putRecords(
                PutRecordsRequest.builder()
                    .streamName(streamName)
                    .records(entries)
                    .build()
            );
            int failed  = resp.failedRecordCount() != null ? resp.failedRecordCount() : 0;
            int success = records.size() - failed;
            if (failed > 0)
                System.out.printf("  [BATCH WARN] %d failed out of %d (records #%d–#%d)%n",
                    failed, records.size(), startNo, startNo + records.size() - 1);
            else
                System.out.printf("  [BATCH OK ] Sent %d records (#%d–#%d)%n",
                    success, startNo, startNo + records.size() - 1);
            return success;
        } catch (KinesisException e) {
            System.out.printf("  [BATCH ERR] %s: %s%n",
                e.awsErrorDetails().errorCode(), e.getMessage());
            return 0;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Record builder — matches screenshot format exactly
    // ─────────────────────────────────────────────────────────────────────────

    static String buildRecord(int recordNo) throws Exception {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        Instant now           = Instant.now();

        // Timestamps
        String tsIso     = ISO_FMT.format(now);
        String tsDisplay = DISPLAY_FMT.format(now);
        String dateStr   = DATE_FMT.format(now);

        // Pick scenario
        String eventType = EVENT_TYPES[rng.nextInt(EVENT_TYPES.length)];
        String status    = pickWeighted(AUTH_STATUSES, STATUS_WEIGHTS, rng);
        String userId    = String.valueOf(rng.nextLong(100_000_000_000L, 999_999_999_999L));
        String ip        = randomIp(rng);
        String hostname  = HOSTNAMES[rng.nextInt(HOSTNAMES.length)];
        String url       = APP_URLS[rng.nextInt(APP_URLS.length)];
        String jvmId     = String.valueOf(rng.nextInt(100_000, 999_999));
        String rolbId    = "ROLB-"
            + rng.nextLong(10_000_000_000_000L, 99_999_999_999_999L)
            + "-" + rng.nextInt(1_000_000, 9_999_999);
        String ibfToken  = UUID.randomUUID().toString();
        String country   = COUNTRIES[rng.nextInt(COUNTRIES.length)];
        String slotName  = randomAlpha(rng, 12) + "-SLOT" + rng.nextInt(1, 9);

        // Session IDs
        int numSessions = rng.nextInt(2, 5);
        List<String> sessionIds = new ArrayList<>(numSessions);
        for (int i = 0; i < numSessions; i++) {
            // Matches screenshot: -1970162495149732183;-1970162495149732183;1763431579243
            sessionIds.add(String.valueOf(
                rng.nextLong(-9_999_999_999_999_999L, 9_999_999_999_999_999L)
            ));
        }

        // Error fields
        boolean isFailed = "FAILED".equals(status) || "LOCKED".equals(status) || "TIMEOUT".equals(status);
        int    errorId   = 0;
        String errorMsg  = null;
        if (isFailed) {
            errorId = ERROR_CODE_KEYS[rng.nextInt(ERROR_CODE_KEYS.length)];
            errorMsg = ERROR_CODES.get(errorId);
        }
        int failedAttempts = isFailed ? rng.nextInt(1, 6) : 0;

        // ── Raw message string — matches screenshot log line ──────────────────
        // "18-11-2025 02:06:19.243 3eelac5bfaf0-SLOT2  ROLB-AWS-ACTIVITY\t195734  N/A  999999...
        //  [IBF-AUTHLO] [unavailable] [26.55.165.88] [ROLB-...] [40bb584a-...]
        //  IN;UNKNOWN;999999999999;<sessionIds>;<ip>;<url>"
        String rawMessage = String.format(
            "%s %s  %s\\t%s\t N/A\t%s [IBF-AUTHLO] [%s] [%s] [%s] [%s] "
                + "%s;%s;%s;%s;%s;%s",
            tsDisplay, slotName, eventType, jvmId, userId,
            status.toLowerCase(), ip, rolbId, ibfToken,
            country, status, userId,
            String.join(";", sessionIds), ip, url
        );

        // ── Assemble JSON ─────────────────────────────────────────────────────
        ObjectNode record = MAPPER.createObjectNode();

        // Core fields (from screenshot)
        record.put("message",      rawMessage);
        record.put("@timestamp",   tsIso);
        record.put("messageType",  "DATA_MESSAGE");

        ArrayNode filters = record.putArray("subscriptionFilters");
        filters.add(SUBSCRIPTION_FILTERS[rng.nextInt(SUBSCRIPTION_FILTERS.length)]);

        record.put("id", randomDigits(rng, 39));

        ObjectNode logstash = record.putObject("logstash");
        logstash.put("hostname", hostname);

        record.put("jvmId",     jvmId);
        record.put("Timestamp", tsDisplay);
        record.put("ifetype",   "app");

        // Screenshot fields: time_interval, event_date, ErrorMsgId
        record.put("time_interval", tsDisplay);
        record.put("event_date",    dateStr);
        record.put("ErrorMsgId",    errorId);
        if (errorMsg != null) record.put("error_detail", errorMsg);
        else                  record.putNull("error_detail");

        // Structured / parsed fields (used by Flink enrichment + Grafana metrics)
        record.put("event_type",      eventType);
        record.put("auth_status",     status);
        record.put("user_id",         userId);
        record.put("ip_address",      ip);
        record.put("country",         country);
        record.put("app_url",         url);
        record.put("rolb_id",         rolbId);
        record.put("ibf_token",       ibfToken);

        ArrayNode sessionsNode = record.putArray("session_ids");
        sessionIds.forEach(sessionsNode::add);

        // Failed auth flags (Flink uses these for enrichment)
        record.put("is_failed_auth",  isFailed);
        record.put("failed_attempts", failedAttempts);

        // Producer metadata
        ObjectNode meta = record.putObject("producer_meta");
        meta.put("sent_at",   Instant.now().toString());
        meta.put("record_no", recordNo);
        meta.put("source",    "kinesis-java-producer");

        return MAPPER.writeValueAsString(record);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Random helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static String pickWeighted(String[] options, int[] weights, ThreadLocalRandom rng) {
        int total = 0;
        for (int w : weights) total += w;
        int r = rng.nextInt(total);
        int cum = 0;
        for (int i = 0; i < options.length; i++) {
            cum += weights[i];
            if (r < cum) return options[i];
        }
        return options[options.length - 1];
    }

    private static String randomIp(ThreadLocalRandom rng) {
        double roll = rng.nextDouble();
        if (roll < 0.6) {
            // Public IP (like 26.55.165.88 in screenshot)
            return rng.nextInt(1, 224) + "." + rng.nextInt(0, 256)
                + "." + rng.nextInt(0, 256) + "." + rng.nextInt(1, 255);
        } else if (roll < 0.8) {
            return "10." + rng.nextInt(0, 256) + "." + rng.nextInt(0, 256) + "." + rng.nextInt(1, 255);
        } else {
            return "192.168." + rng.nextInt(0, 256) + "." + rng.nextInt(1, 255);
        }
    }

    private static String randomAlpha(ThreadLocalRandom rng, int len) {
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(chars.charAt(rng.nextInt(chars.length())));
        return sb.toString();
    }

    private static String randomDigits(ThreadLocalRandom rng, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(rng.nextInt(10));
        return sb.toString();
    }
}
