import json

from kafka import KafkaConsumer, KafkaProducer

SENSOR_TOPIC = "building_sensors_student"
TEMP_TOPIC = "temperature_alerts_student"
HUMIDITY_TOPIC = "humidity_alerts_student"

consumer = KafkaConsumer(
    SENSOR_TOPIC,
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    group_id="sensor_processor"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Processor started. Waiting for sensor data...")

try:
    for message in consumer:
        data = message.value

        temperature = data["temperature"]
        humidity = data["humidity"]

        print("Received:", data)

        if temperature > 40:
            alert = {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "temperature": temperature,
                "message": "Temperature is too high!"
            }
            producer.send(TEMP_TOPIC, value=alert)
            producer.flush()
            print("Temperature alert sent:", alert)

        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "humidity": humidity,
                "message": "Humidity is out of range!"
            }
            producer.send(HUMIDITY_TOPIC, value=alert)
            producer.flush()
            print("Humidity alert sent:", alert)

except KeyboardInterrupt:
    print("Processor stopped")

finally:
    consumer.close()
    producer.close()