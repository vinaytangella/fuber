from fastavro import parse_schema, writer, reader
import io
import json
import time
from pathlib import Path

# 1. Load schema
current_file_path = Path(__file__).resolve()
current_dir = current_file_path.parent
print(current_dir)
file_path = str(current_dir)+"/schemas/ride_requested.avsc"
print(file_path)
with open(file_path, 'r') as f:
    schema = json.load(f)

parsed_schema = parse_schema(schema)

#Create an event
ride_requested_event = {
    "ride_id":"ride_1",
    "user_id":"user_1",
    "pickup_lat":92.3,
    "pickup_lon":-90.7,
    "dropoff_lat":78.3,
    "dropoff_lon":56.7,
    "requested_at":int(time.time() * 1000)
}

# 3. Serialize to Avro (binary)
buffer = io.BytesIO()
writer(buffer, parsed_schema, [ride_requested_event])

avro_bytes = buffer.getvalue()
print(avro_bytes)

# 4. Deserialize back
buffer.seek(0)
records = list(reader(buffer))

print("Deserialized event:", records[0])