"""
Banking Log Record Generator
==============================
Generates fake log records matching the client's IBF-AUTHLO format
and pushes them to a Kinesis stream — used as a Kafka replacement
for the Flink POC in restricted environments.

No JARs needed for this script — it only uses boto3 to talk to Kinesis.

Usage:
    pip install boto3
    python record_generator.py --dry-run          # preview records, no AWS needed
    python record_generator.py                    # send to Kinesis (needs AWS creds)
    python record_generator.py --rate 5           # 5 records/sec
    python record_generator.py --count 50         # send exactly 50 records then stop
"""

import argparse
import boto3
import json
import random
import string
import time
import uuid
from datetime import datetime, timezone


# ──────────────────────────────────────────────────────────────
# Fake data pools  (realistic-looking but all fake)
# ──────────────────────────────────────────────────────────────

ACTIVITY_TYPES = [
    "ROLB-AWS-ACTIVITY",
    "ROLB-AUTH-ACTIVITY",
    "ROLB-SESSION-ACTIVITY",
    "ROLB-LOGOUT-ACTIVITY",
]

AUTH_LABELS = [
    "IBF-AUTHLO",
    "IBF-AUTHHI",
    "IBF-SESS",
    "IBF-LOGOUT",
]

AUTH_STATUS = ["IN", "OUT", "UNKNOWN", "FAILED", "TIMEOUT"]

URL_PATHS = [
    "/olb/authlogin/loginAppContainer.do",
    "/olb/dashboard/home.do",
    "/olb/transfer/initiate.do",
    "/olb/account/summary.do",
    "/olb/logout/session.do",
    "/olb/payment/neft.do",
    "/olb/statement/download.do",
]

HOSTNAMES = [
    "ldwdsr00t3tuhi9",
    "ldwdsr00t4abcd1",
    "ldwdsr00t5efgh2",
    "appserver-prod-01",
    "appserver-prod-02",
]

SUBSCRIPTION_FILTERS = [
    "Send-logs-to-Cloud-Watch-Logs-destination",
    "Send-logs-to-Kinesis-destination",
]


# ──────────────────────────────────────────────────────────────
# Record Generation
# ──────────────────────────────────────────────────────────────

def random_ip():
    return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def random_account():
    return "".join([str(random.randint(0, 9)) for _ in range(12)])


def random_session_id():
    hex_part = "".join(random.choices("abcdef0123456789", k=12))
    slot_num = random.randint(1, 8)
    return f"{hex_part}-SLOT{slot_num}"


def random_rolb_id():
    num1 = random.randint(10000000000000, 99999999999999)
    num2 = random.randint(10000000, 99999999)
    return f"ROLB-{num1}-{num2}"


def random_large_negative():
    return random.randint(-9999999999999999999, -1000000000000000000)


def generate_record():
    """
    Generate one log record matching the client's IBF-AUTHLO JSON format.
    Matches the screenshot structure exactly.
    """
    now = datetime.now(timezone.utc)
    timestamp_str = now.strftime("%d-%m-%Y %H:%M:%S.") + f"{now.microsecond // 1000:03d}"
    iso_timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    session_id     = random_session_id()
    activity_type  = random.choice(ACTIVITY_TYPES)
    jvm_id         = str(random.randint(100000, 999999))
    account_no     = random_account()
    auth_label     = random.choice(AUTH_LABELS)
    client_ip      = random_ip()
    rolb_id        = random_rolb_id()
    correlation_id = str(uuid.uuid4())
    auth_status    = random.choice(AUTH_STATUS)
    url_path       = random.choice(URL_PATHS)
    hostname       = random.choice(HOSTNAMES)
    log_id         = "".join([str(random.randint(0, 9)) for _ in range(38)])

    # Matches the client's exact message format from the screenshot
    message = (
        f"{timestamp_str} {session_id} `{activity_type}`\\t{jvm_id}"
        f"\tN/A\t{account_no}"
        f"\t`[{auth_label}]` [unavailable] [{client_ip}] [{rolb_id}] [{correlation_id}]"
        f"\t{auth_status};UNKNOWN;{account_no}"
        f";;;;;{random_large_negative()};{random_large_negative()}"
        f";{random.randint(1000000000000, 9999999999999)}"
        f";{client_ip};{url_path};`\\n`"
    )

    record = {
        # ── Core fields from client screenshot ──────────────
        "message":             message,
        "@timestamp":          iso_timestamp,
        "messageType":         "DATA_MESSAGE",
        "subscriptionFilters": [random.choice(SUBSCRIPTION_FILTERS)],
        "id":                  log_id,
        "logstash": {
            "hostname": hostname
        },
        "jvmId":               jvm_id,
        "Timestamp":           timestamp_str,
        "ifetype":             "app",

        # ── Extra fields for Flink processing ───────────────
        "account_number":  account_no,
        "client_ip":       client_ip,
        "auth_status":     auth_status,
        "activity_type":   activity_type,
        "url_path":        url_path,
        "session_id":      session_id,
        "correlation_id":  correlation_id,
        "event_date":      now.strftime("%Y-%m-%d"),
        "error_code":      f"ErrorMsg={random.choice(['1101', '2001', '3005', 'None'])} "
                           + "".join(random.choices(string.ascii_lowercase, k=8)),
    }

    return record


# ──────────────────────────────────────────────────────────────
# Kinesis Publisher
# ──────────────────────────────────────────────────────────────

class KinesisPublisher:
    def __init__(self, stream_name: str, region: str):
        self.stream_name = stream_name
        self.client = boto3.client("kinesis", region_name=region)
        print(f"[INFO] Connected to Kinesis stream: '{stream_name}' | region: '{region}'")

    def send(self, record: dict) -> bool:
        try:
            payload       = json.dumps(record, ensure_ascii=False)
            partition_key = record.get("account_number", "default-partition")

            response = self.client.put_record(
                StreamName=self.stream_name,
                Data=payload.encode("utf-8"),
                PartitionKey=partition_key,
            )

            shard_id    = response["ShardId"]
            sequence_no = response["SequenceNumber"][:20] + "..."
            print(
                f"[SENT] account={record['account_number']} "
                f"auth={record['auth_status']} "
                f"ip={record['client_ip']} "
                f"shard={shard_id} seq={sequence_no}"
            )
            return True

        except Exception as e:
            print(f"[ERROR] Failed to send record: {e}")
            return False


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Banking log record generator → Kinesis")
    parser.add_argument("--stream",  default="flink-poc-input-stream", help="Kinesis stream name")
    parser.add_argument("--region",  default="ap-south-1",             help="AWS region")
    parser.add_argument("--rate",    type=float, default=1.0,          help="Records per second")
    parser.add_argument("--count",   type=int,   default=0,            help="Total records (0 = infinite)")
    parser.add_argument("--dry-run", action="store_true",              help="Print records without sending to Kinesis")
    args = parser.parse_args()

    publisher = None if args.dry_run else KinesisPublisher(args.stream, args.region)
    interval  = 1.0 / args.rate
    sent      = 0

    print(f"\n{'='*60}")
    print(f"  Banking Log Generator")
    print(f"  Stream  : {args.stream}")
    print(f"  Region  : {args.region}")
    print(f"  Rate    : {args.rate} record(s)/sec")
    print(f"  Count   : {'infinite' if args.count == 0 else args.count}")
    print(f"  Dry-run : {args.dry_run}")
    print(f"{'='*60}\n")

    try:
        while True:
            record = generate_record()

            if args.dry_run:
                print(json.dumps(record, indent=2))
                print("-" * 60)
            else:
                publisher.send(record)

            sent += 1
            if args.count > 0 and sent >= args.count:
                print(f"\n[DONE] Sent {sent} records. Exiting.")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n[STOPPED] Total records sent: {sent}")


if __name__ == "__main__":
    main()
