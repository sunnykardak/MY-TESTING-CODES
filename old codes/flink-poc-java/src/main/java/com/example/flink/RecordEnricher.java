package com.example.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * RecordEnricher
 * ─────────────────────────────────────────────────────────────────────────────
 * Flink MapFunction that:
 *   1. Parses each raw JSON record from Kinesis
 *   2. Copies all original fields (message, @timestamp, jvmId, logstash, etc.)
 *   3. Adds enrichment fields:
 *        processingTimestamp  — when Flink processed the record
 *        isFailedAuth         — true if auth event failed or failedAttempts >= 3
 *        isSuspicious         — true if any fraud/risk rule triggered
 *        suspiciousReasons    — semicolon-separated list of triggered rules
 *        riskScore            — numeric score (0–100+)
 *        riskLevel            — NONE / LOW / MEDIUM / HIGH / CRITICAL
 *   4. On parse error: wraps in a structured error envelope so nothing is lost
 *
 * Input record shape (produced by KinesisRecordProducer):
 * {
 *   "message":       "<raw log line>",
 *   "@timestamp":    "2025-11-18T02:06:19.244Z",
 *   "messageType":   "DATA_MESSAGE",
 *   "jvmId":         "195734",
 *   "Timestamp":     "18-11-2025 02:06:19.243",
 *   "time_interval": "18-11-2025 02:06:19.243",
 *   "event_date":    "2025-11-18",
 *   "ErrorMsgId":    1101,
 *   "error_detail":  "Invalid credentials supplied",
 *   "event_type":    "ROLB-AWS-ACTIVITY",
 *   "auth_status":   "FAILED",
 *   "user_id":       "999999999999",
 *   "ip_address":    "26.55.165.88",
 *   "country":       "IN",
 *   "is_failed_auth": true,
 *   "failed_attempts": 3,
 *   ...
 * }
 * ─────────────────────────────────────────────────────────────────────────────
 */
public class RecordEnricher implements MapFunction<String, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RecordEnricher.class);

    // ── Thresholds ────────────────────────────────────────────────────────────
    private static final int    FAILED_AUTH_THRESHOLD   = 3;
    private static final double HIGH_TRANSACTION_AMOUNT = 10_000.0;
    private static final String RESTRICTED_COUNTRIES    = "NK,IR,CU";

    // ObjectMapper is not serialisable — mark transient, init on first use
    private transient ObjectMapper mapper;
    private transient DateTimeFormatter isoFmt;

    private ObjectMapper getMapper() {
        if (mapper == null) mapper = new ObjectMapper();
        return mapper;
    }

    private DateTimeFormatter getIsoFmt() {
        if (isoFmt == null)
            isoFmt = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                .withZone(ZoneOffset.UTC);
        return isoFmt;
    }

    // ─────────────────────────────────────────────────────────────────────────
    @Override
    public String map(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            LOG.warn("Received blank record — skipping");
            return null;
        }
        try {
            return enrich(rawJson);
        } catch (Exception e) {
            LOG.error("Failed to enrich record | error={} | raw={}", e.getMessage(), rawJson, e);
            return buildErrorEnvelope(rawJson, e.getMessage());
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    private String enrich(String rawJson) throws Exception {
        ObjectMapper om   = getMapper();
        JsonNode     input = om.readTree(rawJson);

        // Must be an object node to enrich
        if (!input.isObject()) {
            LOG.warn("Record is not a JSON object — wrapping as-is");
            return buildErrorEnvelope(rawJson, "not a JSON object");
        }

        ObjectNode enriched = input.deepCopy();

        // ── Processing metadata ───────────────────────────────────────────────
        String now = getIsoFmt().format(Instant.now());
        enriched.put("processingTimestamp", now);
        enriched.put("processingHost",      "aws-managed-flink");
        enriched.put("appVersion",          "1.0.0");

        // ── Normalise status to uppercase ─────────────────────────────────────
        String status = textOrDefault(input, "auth_status", "UNKNOWN").toUpperCase();
        enriched.put("auth_status", status);

        // ── Failed-auth flag ──────────────────────────────────────────────────
        boolean failedAuth = detectFailedAuth(input, status);
        enriched.put("isFailedAuth", failedAuth);

        // ── Suspicious-activity detection ─────────────────────────────────────
        RiskResult risk = detectRisk(input, status);
        enriched.put("isSuspicious",      risk.isSuspicious);
        enriched.put("suspiciousReasons", risk.reasons);
        enriched.put("riskScore",         risk.score);
        enriched.put("riskLevel",         riskLevel(risk.score));

        return om.writeValueAsString(enriched);
    }

    // ── Failed-auth detection ─────────────────────────────────────────────────
    private boolean detectFailedAuth(JsonNode node, String status) {
        // Explicit failed auth event
        if ("FAILED".equals(status) || "LOCKED".equals(status)) return true;

        // Repeated failures field
        int attempts = node.has("failed_attempts")
            ? node.get("failed_attempts").asInt(0) : 0;
        if (attempts >= FAILED_AUTH_THRESHOLD) return true;

        // ErrorMsgId in auth-failure range (1101–1104)
        int errorId = node.has("ErrorMsgId") ? node.get("ErrorMsgId").asInt(0) : 0;
        return errorId >= 1101 && errorId <= 1104;
    }

    // ── Risk / suspicious-activity detection ──────────────────────────────────
    private RiskResult detectRisk(JsonNode node, String status) {
        StringBuilder reasons = new StringBuilder();
        int score = 0;

        // Rule 1 — high-value transaction amount
        if (node.has("amount")) {
            double amount = node.get("amount").asDouble(0);
            if (amount >= HIGH_TRANSACTION_AMOUNT) {
                score += 30;
                append(reasons, "HIGH_TRANSACTION_AMOUNT:" + amount);
            }
        }

        // Rule 2 — restricted / sanctioned country
        String country = textOrDefault(node, "country", "").toUpperCase();
        if (!country.isEmpty() && RESTRICTED_COUNTRIES.contains(country)) {
            score += 40;
            append(reasons, "RESTRICTED_COUNTRY:" + country);
        }

        // Rule 3 — repeated failed auth attempts
        int attempts = node.has("failed_attempts")
            ? node.get("failed_attempts").asInt(0) : 0;
        if (attempts >= FAILED_AUTH_THRESHOLD) {
            score += Math.min(attempts, 10) * 5; // 5 pts per attempt, cap at 50
            append(reasons, "REPEATED_FAILED_AUTH:" + attempts);
        }

        // Rule 4 — internal RFC-1918 IP on a transaction event
        String ip        = textOrDefault(node, "ip_address", "");
        String eventType = textOrDefault(node, "event_type",  "");
        if (isInternalIp(ip) && eventType.contains("TXN")) {
            score += 10;
            append(reasons, "INTERNAL_IP_ON_TXN:" + ip);
        }

        // Rule 5 — failed transaction (not just auth)
        if (eventType.contains("TXN") && "FAILED".equals(status)) {
            score += 20;
            append(reasons, "FAILED_TRANSACTION");
        }

        // Rule 6 — error in 1200-range (transaction errors)
        int errorId = node.has("ErrorMsgId") ? node.get("ErrorMsgId").asInt(0) : 0;
        if (errorId >= 1200) {
            score += 15;
            append(reasons, "TRANSACTION_ERROR_CODE:" + errorId);
        }

        // Rule 7 — account locked
        if ("LOCKED".equals(status)) {
            score += 25;
            append(reasons, "ACCOUNT_LOCKED");
        }

        return new RiskResult(score > 0, reasons.toString(), score);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    private static String riskLevel(int score) {
        if (score >= 70) return "CRITICAL";
        if (score >= 40) return "HIGH";
        if (score >= 20) return "MEDIUM";
        if (score >  0)  return "LOW";
        return "NONE";
    }

    private static String textOrDefault(JsonNode node, String field, String def) {
        return (node.has(field) && !node.get(field).isNull())
            ? node.get(field).asText(def) : def;
    }

    private static boolean isInternalIp(String ip) {
        if (ip == null || ip.isEmpty()) return false;
        return ip.startsWith("10.")
            || ip.startsWith("192.168.")
            || ip.startsWith("172.16.") || ip.startsWith("172.17.")
            || ip.startsWith("172.18.") || ip.startsWith("172.19.")
            || ip.startsWith("172.2")
            || ip.startsWith("172.30.") || ip.startsWith("172.31.");
    }

    private static void append(StringBuilder sb, String reason) {
        if (sb.length() > 0) sb.append("; ");
        sb.append(reason);
    }

    private String buildErrorEnvelope(String rawJson, String errorMessage) {
        try {
            ObjectNode err = getMapper().createObjectNode();
            err.put("processingError",     true);
            err.put("errorMessage",        errorMessage);
            err.put("rawPayload",          rawJson);
            err.put("processingTimestamp", getIsoFmt().format(Instant.now()));
            return getMapper().writeValueAsString(err);
        } catch (Exception ex) {
            return "{\"processingError\":true,\"errorMessage\":\"serialization-failure\"}";
        }
    }

    // ── Inner value type ──────────────────────────────────────────────────────
    static class RiskResult {
        final boolean isSuspicious;
        final String  reasons;
        final int     score;

        RiskResult(boolean isSuspicious, String reasons, int score) {
            this.isSuspicious = isSuspicious;
            this.reasons      = reasons;
            this.score        = score;
        }
    }
}
