import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "temperature_alerts_student",
    "humidity_alerts_student",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="alerts_consumer"
)

print("Alert consumer started. Waiting for alerts...")

try:
    for message in consumer:
        print("\n🚨 ALERT RECEIVED:")
        print(message.value)

except KeyboardInterrupt:
    print("Alert consumer stopped")

finally:
    consumer.close()