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
    "ssl.ca.location": f"{BASE}\\intermediate_ca.crt",
    "ssl.certificate.location": f"{BASE}\\tls.crt",
    "ssl.key.location": f"{BASE}\\tls.key",
    "ssl.key.password": "bukpass",
    "ssl.endpoint.identification.algorithm": "none",
    "auto.offset.reset": "earliest",
    "session.timeout.ms": 15000,
}
