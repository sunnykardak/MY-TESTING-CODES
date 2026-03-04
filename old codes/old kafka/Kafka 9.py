from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka1.bsp.buk.421850845486.aws.intranet:9092") \
    .set_topics("buk-sdwh-digital-poc") \
    .set_group_id("flink-group") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .set_property("security.protocol", "SSL") \
    .set_property("ssl.ca.location", "/tmp/ca.crt") \
    .set_property("ssl.certificate.location", "/tmp/client.crt") \
    .set_property("ssl.key.location", "/tmp/client.key") \
    .set_property("ssl.key.password", "bukpass") \
    .build()

ds = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)

ds.print()

env.execute("BSP Kafka Flink Test")
