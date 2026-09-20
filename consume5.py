from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "server_metrics",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="aiops-consumer"
)

anomaly_count = 0

print("AIOps Monitoring Started...")
print("Listening for server metrics...\n")

for message in consumer:
    data = json.loads(message.value.decode("utf-8"))

    server_id = data["server_id"]
    cpu = data["cpu_usage"]

    print(f"Message received: {server_id} | CPU: {cpu}%")

    if cpu > 80:
        anomaly_count += 1
        print("ALERT: High CPU detected")
    else:
        print("Normal")

    print(f"Total anomalies detected: {anomaly_count}")
    print()