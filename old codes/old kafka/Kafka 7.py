from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

producer = Producer(conf)

producer.produce(
    "buk-sdwh-digital-poc",
    key="test-key",
    value="Hello Barclays Test",
    callback=delivery_report
)

producer.poll(1)
producer.flush()
