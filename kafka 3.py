Step 1 — Extract CA from your truststore in PowerShell:
powershellcd C:\devhome\projects\working-flink-python

openssl pkcs12 -in truststore-bsp-compact.p12 -nokeys -out ca-issuing-bundle.crt -passin pass:mystorepassword -legacy
If -legacy doesn't work try:
powershellopenssl pkcs12 -in truststore-bsp-compact.p12 -nokeys -out ca-issuing-bundle.crt -passin pass:mystorepassword

Step 2 — Extract cert and key from keystore_suresh.p12:
powershell# Extract certificate
openssl pkcs12 -in keystore_suresh.p12 -clcerts -nokeys -out client.crt -passin pass:bukpass -legacy

# Extract private key
openssl pkcs12 -in keystore_suresh.p12 -nocerts -nodes -out client.key -passin pass:bukpass -legacy

Step 3 — Run Python with extracted files:
pythonimport socket
from confluent_kafka import Consumer, KafkaException

_original_getaddrinfo = socket.getaddrinfo
def _custom_getaddrinfo(host, port, *args, **kwargs):
    if "kafka.bsp.buk" in str(host):
        host = "28.45.10.24"
    return _original_getaddrinfo(host, port, *args, **kwargs)
socket.getaddrinfo = _custom_getaddrinfo

BASE = r"C:\devhome\projects\working-flink-python"

conf = {
    "bootstrap.servers": "kafka.bsp.buk.421850845486.aws.intranet:9092",
    "group.id": "snsvc0067919",
    "security.protocol": "SSL",
    "ssl.ca.location": f"{BASE}\\ca-issuing-bundle.crt",
    "ssl.certificate.location": f"{BASE}\\client.crt",
    "ssl.key.location": f"{BASE}\\client.key",
    "ssl.key.password": "bukpass",
    "ssl.endpoint.identification.algorithm": "none",
    "auto.offset.reset": "earliest",
    "session.timeout.ms": 15000,
}

print("Connecting to BSP Kafka...")

try:
    consumer = Consumer(conf)
    consumer.subscribe(["buk-sdwh-digital-poc"])
    print("Subscribed! Waiting for messages...")
    count = 0
    while count < 5:
        msg = consumer.poll(timeout=10.0)
        if msg is None:
            print("Waiting...")
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            break
        else:
            print(f"✅ Message {count+1}:")
            print(msg.value().decode('utf-8')[:300])
            count += 1
except KafkaException as e:
    print(f"Failed: {e}")
finally:
    try:
        consumer.close()
    except:
        pass
    print("Done.")
