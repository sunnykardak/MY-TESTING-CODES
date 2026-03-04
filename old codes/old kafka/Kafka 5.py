from confluent_kafka import Producer
import socket

# ---- DNS Override (Keep only if needed in your env) ----
_original_getaddrinfo = socket.getaddrinfo
def _custom_getaddrinfo(host, port, *args, **kwargs):
    if "kafka.bsp.buk" in str(host):
        host = "28.45.10.24"
    return _original_getaddrinfo(host, port, *args, **kwargs)

socket.getaddrinfo = _custom_getaddrinfo
# ---------------------------------------------------------

BASE = r"C:\devhome\projects\working-flink-python"

conf = {
    "bootstrap.servers": "kafka1.bsp.buk.421850845486.aws.intranet:9092",
    "security.protocol": "SSL",
    "ssl.ca.location": f"{BASE}\\ca-issuing-bundle.crt",
    "ssl.certificate.location": f"{BASE}\\client.crt",
    "ssl.key.location": f"{BASE}\\client.key",
    "ssl.key.password": "bukpass",
    "ssl.endpoint.identification.algorithm": "none"
}

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

print("Connecting Producer...")

producer = Producer(conf)

topic = "buk-sdwh-digital-poc"

try:
    producer.produce(topic, key="test-key", value="Hello from Barclays Producer Test")
    producer.flush()
except Exception as e:
    print(f"Failed: {e}")

print("Done.")
