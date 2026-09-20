from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "server_metrics",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="server-metrics-display"
)

print("Connected to Kafka broker")
print("Listening for server metrics...\n")

for message in consumer:
    data = json.loads(message.value.decode("utf-8"))

    server_id = data["server_id"]
    cpu = data["cpu_usage"]
    memory = data["memory_usage"]

    print("Received:")
    print(f"Server: {server_id}")
    print(f"CPU: {cpu}%")
    print(f"Memory: {memory}%")
    print()