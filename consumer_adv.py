from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import authlib

# 1. Schema Registry client
schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(schema_registry_client)

# 2. Consumer config
consumer_conf = {
    "bootstrap.servers": "localhost:29092",
    "group.id": "ride-consumer",
    "auto.offset.reset": "earliest",
    "value.deserializer": avro_deserializer
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(["ride_requested"])

print("Waiting for messages...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    print("Consumed event:", msg.value())
