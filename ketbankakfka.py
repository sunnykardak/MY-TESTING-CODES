import socket
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

    # Using Ketan's exact files
    "ssl.ca.location": f"{BASE}\\truststore-bsp-compact.p12",
    "ssl.certificate.location": f"{BASE}\\tls.crt",
    "ssl.key.location": f"{BASE}\\tls.key",
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
            print(f"Message {count+1}: {msg.value().decode('utf-8')[:300]}")
            count += 1

except KafkaException as e:
    print(f"Failed: {e}")
finally:
    try:
        consumer.close()
    except:
        pass
    print("Done.")
