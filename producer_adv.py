from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json
import time
from pathlib import Path

# 1. Schema Registry client
schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 2. Load schema
current_file_path = Path(__file__).resolve()
current_dir = current_file_path.parent
file_path = str(current_dir)+"/schemas/ride_requested.avsc"
with open(file_path, 'r') as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)

# 3. Producer config
producer_conf = {
    "bootstrap.servers": "localhost:29092",
    "value.serializer": avro_serializer
}

producer = SerializingProducer(producer_conf)

# 4. Event
event = {
    "ride_id": "ride_100",
    "user_id": "user_900",
    "pickup_lat": 37.77,
    "pickup_lon": -122.41,
    "dropoff_lat": 37.78,
    "dropoff_lon": -122.40,
    "requested_at": int(time.time() * 1000),
    "ride_type": "standard"
}

# 5. Produce
producer.produce(topic="ride_requested", value=event)
producer.flush()

print("Produced event with schema registry")