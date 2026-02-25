import socket

# Fix DNS resolution in Command Prompt
_original_getaddrinfo = socket.getaddrinfo
def _custom_getaddrinfo(host, port, *args, **kwargs):
    if "kafka.bsp.buk" in str(host):
        host = "28.45.10.24"
    return _original_getaddrinfo(host, port, *args, **kwargs)
socket.getaddrinfo = _custom_getaddrinfo

from confluent_kafka import Consumer, KafkaException

conf = {
    "bootstrap.servers": "kafka.bsp.buk.421850845486.aws.intranet:9092",
    "group.id": "nishikesh-test-consumer-001",
    "security.protocol": "SSL",
    
    # Certificate files
    "ssl.ca.location": "root_ca.crt",
    "ssl.certificate.location": "tls.crt",
    "ssl.key.location": "tls.key",
    "ssl.key.password": "1122",
    
    # Disable hostname verification since we use IP
    "ssl.endpoint.identification.algorithm": "none",
    
    "auto.offset.reset": "earliest",
    "session.timeout.ms": 15000,
    "request.timeout.ms": 20000,
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
            print("Waiting for messages...")
            continue
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            break
        else:
            print(f"Message {count+1} received!")
            print(msg.value().decode('utf-8')[:300])
            print("---")
            count += 1

except KafkaException as e:
    print(f"Connection failed: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    consumer.close()
    print("Done.")



& "C:\devhome\tools\python3.9\current\python.exe" "C:\devhome\projects\working-flink-python\test_bsp_kafka.py"

