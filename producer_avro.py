from kafka import KafkaProducer
from fastavro import writer, reader, parse_schema
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
#parse the schema
parsed_schema = parse_schema(data)
buffer = io.BytesIO()

#create an event
ride_requested_event = {
    "ride_id":"ride_1",
    "user_id":"user_1",
    "pickup_lat":92.3,
    "pickup_lon":-90.7,
    "dropoff_lat":78.3,
    "dropoff_lon":56.7,
    "requested_at":int(time.time() * 1000)
}

writer(buffer,parsed_schema,[ride_requested_event])

producer = KafkaProducer(bootstrap_servers="http://localhost:9092")

avro_bytes = buffer.getvalue()

producer.send("ride_requested",avro_bytes)
producer.flush()