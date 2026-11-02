import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

TOPIC = "building_sensors_student"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

sensor_id = random.randint(1000, 9999)

print(f"Sensor {sensor_id} started")

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().isoformat(),
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2)
        }

        producer.send(TOPIC, value=data)
        producer.flush()

        print("Sent:", data)
        time.sleep(5)

except KeyboardInterrupt:
    print("Sensor stopped")

finally:
    producer.close()