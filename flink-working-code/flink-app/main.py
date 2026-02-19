"""
PyFlink Banking Log Processor
==============================
Reads IBF-AUTHLO log records from Kinesis,
processes them, and writes output to S3 as JSON files.
CloudWatch logs are automatically captured by AWS Managed Flink.

Architecture:
    Kinesis (input) → PyFlink → S3 (output JSON files)
                             → CloudWatch Logs (auto via Managed Flink)

Flink Version  : 1.16
Connector JARs : flink-connector-kinesis-4.0.0-1.16.jar  (from client Nexus)
                 flink-s3-fs-hadoop-1.16.1.jar            (from client Nexus)

LOCAL testing  : Place JARs in  flink-poc/jars/
AWS deployment : Upload JARs to s3://your-bucket/jars/
                 Upload this file zipped to s3://your-bucket/python/flink-python-app.zip
"""

import json
import logging
import os
from datetime import datetime

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    FilterFunction,
    MapFunction,
    RuntimeContext,
    RichMapFunction,
)

# AWS Managed Flink runtime properties
try:
    from amazonaws.kinesis.analytics.runtime import KinesisAnalyticsRuntime
    IS_AWS = True
except ImportError:
    IS_AWS = False

# ──────────────────────────────────────────────────────────────
# Logging — goes to CloudWatch automatically on AWS Managed Flink
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("BankingLogProcessor")


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "input_stream":    "flink-poc-input-stream",
    "output_bucket":   "flink-poc-output-bucket",   # your S3 bucket name
    "output_prefix":   "processed-logs",
    "region":          "ap-south-1",
    "stream_position": "LATEST",                    # LATEST or TRIM_HORIZON
}


def get_config():
    """
    Load config from AWS Managed Flink runtime properties if on AWS,
    otherwise fall back to defaults for local testing.
    """
    if IS_AWS:
        try:
            props = KinesisAnalyticsRuntime.get_application_properties()
            app_props = props.get("ApplicationProperties", {})
            return {
                "input_stream":    app_props.get("input.stream.name",   DEFAULT_CONFIG["input_stream"]),
                "output_bucket":   app_props.get("output.bucket.name",  DEFAULT_CONFIG["output_bucket"]),
                "output_prefix":   app_props.get("output.prefix",       DEFAULT_CONFIG["output_prefix"]),
                "region":          app_props.get("aws.region",          DEFAULT_CONFIG["region"]),
                "stream_position": app_props.get("stream.position",     DEFAULT_CONFIG["stream_position"]),
            }
        except Exception as e:
            logger.warning(f"Could not load AWS runtime properties: {e}. Using defaults.")
    return DEFAULT_CONFIG


# ──────────────────────────────────────────────────────────────
# Processing Functions
# ──────────────────────────────────────────────────────────────

class ValidJsonFilter(FilterFunction):
    """Drop records that are not valid JSON or are empty."""

    def filter(self, value: str) -> bool:
        if not value or not value.strip():
            return False
        try:
            json.loads(value)
            return True
        except json.JSONDecodeError:
            logger.warning(f"Dropping invalid JSON: {value[:100]}")
            return False


class BankingLogEnricher(RichMapFunction):
    """
    Enriches each banking log record with:
    - processing_timestamp
    - is_failed_auth  (True if auth_status is FAILED/UNKNOWN/TIMEOUT)
    - is_suspicious   (failed auth on sensitive payment/transfer URLs)
    - flink_subtask_id (which Flink slot processed this record)
    """

    def open(self, runtime_context: RuntimeContext):
        self.task_index = runtime_context.get_index_of_this_subtask()
        logger.info(f"Enricher started on subtask {self.task_index}")

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)

            # ── Enrichment fields ───────────────────────────
            record["processing_timestamp"] = datetime.utcnow().isoformat() + "Z"
            record["flink_subtask_id"]     = self.task_index

            auth_status = record.get("auth_status", "").upper()
            record["is_failed_auth"] = auth_status in ("FAILED", "UNKNOWN", "TIMEOUT")
            record["is_successful"]  = auth_status == "IN"

            # Flag suspicious: failed login on sensitive URL
            sensitive_paths = ["/olb/transfer/initiate.do", "/olb/payment/neft.do"]
            record["is_suspicious"] = (
                record["is_failed_auth"]
                and record.get("url_path", "") in sensitive_paths
            )

            # Parse event_date from Timestamp if not present
            if "event_date" not in record:
                ts = record.get("Timestamp", record.get("@timestamp", ""))
                record["event_date"] = ts[:10] if ts else "unknown"

            # ── CloudWatch log line ─────────────────────────
            log_msg = (
                f"PROCESSED | account={record.get('account_number', 'N/A')} "
                f"auth={auth_status} "
                f"ip={record.get('client_ip', 'N/A')} "
                f"path={record.get('url_path', 'N/A')} "
                f"failed={record['is_failed_auth']} "
                f"suspicious={record['is_suspicious']}"
            )
            logger.info(log_msg)

            if record["is_suspicious"]:
                logger.warning(f"SUSPICIOUS ACTIVITY DETECTED: {log_msg}")

            return json.dumps(record, ensure_ascii=False)

        except Exception as e:
            logger.error(f"Error enriching record: {e} | raw={value[:200]}")
            return json.dumps({
                "processing_error":     str(e),
                "raw_record":           value[:500],
                "processing_timestamp": datetime.utcnow().isoformat() + "Z",
                "status":               "PROCESSING_FAILED",
            })


class SuspiciousActivityLogger(MapFunction):
    """Extra logging pass — logs suspicious records with ALERT prefix."""

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
            if record.get("is_suspicious"):
                logger.warning(
                    f"[ALERT] Suspicious transaction: "
                    f"account={record.get('account_number')} "
                    f"ip={record.get('client_ip')} "
                    f"path={record.get('url_path')} "
                    f"timestamp={record.get('@timestamp')}"
                )
        except Exception:
            pass
        return value


# ──────────────────────────────────────────────────────────────
# Main Application
# ──────────────────────────────────────────────────────────────

def main():
    config = get_config()
    logger.info(f"Starting BankingLogProcessor | Flink 1.16 | config: {config}")

    # ── 1. Execution Environment ────────────────────────────
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(60_000)  # checkpoint every 60 seconds

    # ── 2. Add Connector JARs ───────────────────────────────
    # On AWS Managed Flink → JARs are loaded from S3 via CloudFormation
    #   property: kinesis.analytics.flink.run.options → jarfile
    #   S3 path : s3://your-bucket/jars/flink-connector-kinesis-4.0.0-1.16.jar
    #
    # For LOCAL testing → JARs are loaded from flink-poc/jars/ folder
    if not IS_AWS:
        # flink-poc/jars/ — put your downloaded JARs here for local testing
        jar_dir     = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "jars"))
        kinesis_jar = os.path.join(jar_dir, "flink-connector-kinesis-4.0.0-1.16.jar")
        s3_jar      = os.path.join(jar_dir, "flink-s3-fs-hadoop-1.16.1.jar")

        if os.path.exists(kinesis_jar):
            env.add_jars(f"file://{kinesis_jar}")
            logger.info(f"Loaded JAR (local): {kinesis_jar}")
        else:
            logger.warning(f"JAR not found — download to flink-poc/jars/: {kinesis_jar}")

        if os.path.exists(s3_jar):
            env.add_jars(f"file://{s3_jar}")
            logger.info(f"Loaded JAR (local): {s3_jar}")
        else:
            logger.warning(f"JAR not found — download to flink-poc/jars/: {s3_jar}")

    # ── 3. Kinesis Source Properties ────────────────────────
    kinesis_props = {
        "aws.region":           config["region"],
        "flink.stream.initpos": config["stream_position"],
        # AWS Managed Flink uses IAM role credentials automatically.
        # Local testing: set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
    }

    # ── 4. Kinesis Source ───────────────────────────────────
    from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer

    kinesis_source = FlinkKinesisConsumer(
        streams=config["input_stream"],
        deserializer=SimpleStringSchema(),
        config_props=kinesis_props,
    )

    # ── 5. Build the Pipeline ───────────────────────────────
    stream = (
        env
        .add_source(kinesis_source, source_name="Kinesis-Input")
        .filter(ValidJsonFilter())
        .name("Filter-Valid-JSON")
        .map(BankingLogEnricher(), output_type=Types.STRING())
        .name("Enrich-Banking-Logs")
        .map(SuspiciousActivityLogger(), output_type=Types.STRING())
        .name("Log-Suspicious-Activity")
    )

    # ── 6. S3 Sink (StreamingFileSink) ──────────────────────
    # Requires: flink-s3-fs-hadoop-1.16.1.jar
    # Files roll every 5 minutes or 128MB.
    # Visible in S3 after first checkpoint (~60 seconds).
    from pyflink.datastream.connectors.file_system import (
        StreamingFileSink,
        RollingPolicy,
        OutputFileConfig,
    )
    from pyflink.common.serialization import Encoder

    s3_path = f"s3a://{config['output_bucket']}/{config['output_prefix']}"
    logger.info(f"S3 output path: {s3_path}")

    s3_sink = (
        StreamingFileSink
        .for_row_format(
            base_path=s3_path,
            encoder=Encoder.simple_string_encoder("UTF-8"),
        )
        .with_rolling_policy(
            RollingPolicy.default_rolling_policy(
                part_size=128 * 1024 * 1024,     # 128 MB
                rollover_interval=5 * 60 * 1000,  # 5 minutes
                inactivity_interval=60 * 1000,    # 1 minute idle
            )
        )
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("banking-logs")
            .with_part_suffix(".json")
            .build()
        )
        .build()
    )

    stream.add_sink(s3_sink).name("S3-Output")

    # ── 7. Execute ──────────────────────────────────────────
    logger.info("Submitting Flink job: BankingLogProcessor")
    env.execute("BankingLogProcessor")


if __name__ == "__main__":
    main()
