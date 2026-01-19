from kafka import KafkaConsumer
from fastavro import schemaless_reader, parse_schema, reader
import io
from pathlib import Path
import time
import json

#load the schema
current_file_path = Path(__file__).resolve()
current_dir = current_file_path.parent
file_path = str(current_dir)+"/schemas/ride_requested.avsc"
with open(file_path,"r") as wr:
    data = json.load(wr)

parsed_schema = parse_schema(data)

kafka_consumer = KafkaConsumer("ride_requested", bootstrap_servers="localhost:9092", auto_offset_reset="earliest", enable_auto_commit=True,group_id="ride-service")

#consumer is now ready
print("Waiting for messages...")

for message in kafka_consumer:
    avro_bytes = message.value

    buffer = io.BytesIO(avro_bytes)

    event = schemaless_reader(buffer, parsed_schema)
    # event = buffer.seek(0)
    # records = list(reader(buffer))

    print('Received Event',event)
