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

for i in range(10):

    message = {
        "server_id": f"server{i+1}",
        "cpu_usage": 50 + i * 4,
        "memory_usage": 60 + i
    }

    producer.send(
        "server_metrics1",
        value=message
    )

    print("Sent:", message)

    time.sleep(1)

producer.flush()
producer.close()