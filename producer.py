# brew install kafka
# brew services start kafka
# python3 -m pip install kafka-python

# kafka-topics --create \
#   --topic server_metrics \
#   --bootstrap-server localhost:9092 \
#   --partitions 1 \
#   --replication-factor 1

# kafka-topics --list \
#   --bootstrap-server localhost:9092

# kafka-console-consumer --topic server_metrics --bootstrap-server localhost:9092 --from-beginning

# source .venv/bin/activate

from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

messages = [
    {"server_id": "server01", "cpu_usage": 82, "memory_usage": 65},
    {"server_id": "server02", "cpu_usage": 45, "memory_usage": 55},
    {"server_id": "server03", "cpu_usage": 71, "memory_usage": 60},
    {"server_id": "server04", "cpu_usage": 90, "memory_usage": 78},
    {"server_id": "server05", "cpu_usage": 63, "memory_usage": 52},
    {"server_id": "server01", "cpu_usage": 95, "memory_usage": 82},
    {"server_id": "server02", "cpu_usage": 58, "memory_usage": 61},
    {"server_id": "server03", "cpu_usage": 76, "memory_usage": 69},
    {"server_id": "server04", "cpu_usage": 88, "memory_usage": 73},
    {"server_id": "server05", "cpu_usage": 92, "memory_usage": 85}
]

for message in messages:
    producer.send("server_metrics", value=message)
    print("Sent:", message)
    time.sleep(1)

producer.flush()
producer.close()

print("All messages sent successfully!")