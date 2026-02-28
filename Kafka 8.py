import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy

# Download certs from S3 to local runtime
os.system("aws s3 cp s3://flink-poc-app-nishikesh/certs/client.crt /tmp/client.crt")
os.system("aws s3 cp s3://flink-poc-app-nishikesh/certs/client.key /tmp/client.key")
os.system("aws s3 cp s3://flink-poc-app-nishikesh/certs/ca-issuing-bundle.crt /tmp/ca.crt")

env = StreamExecutionEnvironment.get_execution_environment()

source = KafkaSource.builder() \
    .set_bootstrap_servers("YOUR_KAFKA_HOST:9092") \
    .set_topics("YOUR_TOPIC_NAME") \
    .set_group_id("flink-test-group") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .set_property("security.protocol", "SSL") \
    .set_property("ssl.ca.location", "/tmp/ca.crt") \
    .set_property("ssl.certificate.location", "/tmp/client.crt") \
    .set_property("ssl.key.location", "/tmp/client.key") \
    .build()

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")

ds.print()

env.execute("Kafka-Flink-Test")
